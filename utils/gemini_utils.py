import os
import logging
from typing import Dict, Any, List, Optional

from langchain_google_genai import GoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from langchain.schema import StrOutputParser
from pydantic import SecretStr
from consts import DIMENSION

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize Gemini API
API_KEY = os.environ.get("GEMINI_API_KEY", "")
if not API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable not set")

# Initialize LangChain models
llm = GoogleGenerativeAI(model="gemini-1.5-pro", google_api_key=API_KEY)
embedding_model = GoogleGenerativeAIEmbeddings(
    model="models/gemini-embedding-exp-03-07", google_api_key=SecretStr(API_KEY)
)


# Define Pydantic models for structured data extraction
class UniversityCourseData(BaseModel):
    class University(BaseModel):
        university_name: str = Field(description="Name of the university")
        university_description: str = Field(description="Description of the university")
        country: str = Field(description="Country where the university is located")
        course_name: str = Field(description="Name of the course")
        description: str = Field(description="Description of the course")
        degree_type: str = Field(description="Type of degree (bachelor or masters)")
        starting_date: str = Field(description="Starting date of the course")
        duration: str = Field(description="Duration of the course")
        scholarship: str = Field(description="Scholarship information if available")
        fee_structure: Optional[str] = Field(
            description="Fee structure of the course", default=None
        )
        language_of_study: str = Field(
            description="Language in which the course is taught"
        )
        field_of_study: str = Field(description="Field of study for the course")

    universities: List[University] = []


class QueryIntent(BaseModel):
    requires_lookup: bool = Field(
        description="Whether the query requires university/course data lookup"
    )
    reason: str = Field(description="Reason for the classification")
    target: str = Field(description="What the query is about: 'university', 'course', or 'both'")


def extract_structured_data(content: str) -> Optional[Dict[str, Any]]:
    """
    Extract structured university and course data from raw content using LangChain.

    Args:
        content: Raw content from scraped file

    Returns:
        Dictionary containing structured data or None if extraction failed
    """
    try:
        # Create a parser for the structured data
        parser = PydanticOutputParser(pydantic_object=UniversityCourseData)

        # Create a prompt template
        prompt = PromptTemplate(
            template="""
            Extract structured university and course information from the following content.
            If the content doesn't contain university or course information, return empty values.
            
            Content:
            {content}
            
            {format_instructions}
            """,
            input_variables=["content"],
            partial_variables={"format_instructions": parser.get_format_instructions()},
        )

        # Create and run the chain
        chain = prompt | llm | parser

        result = chain.invoke({"content": content})
        return result.dict()

    except Exception as e:
        logger.error(f"Error extracting structured data: {str(e)}")
        return None


def generate_embedding(text: str, task_type: str) -> Optional[List[float]]:
    """
    Generate embedding vector for text using LangChain.
    Args:
        text: Text to generate embedding for
    Returns:
        Embedding vector as a list of floats, or None if generation failed
    """
    try:
        # Generate embedding using LangChain
        embedding_list = embedding_model.embed_query(
            text, task_type=task_type, output_dimensionality=DIMENSION
        )
        logger.info(f"Generated embedding with dimension: {len(embedding_list)}")
        # Ensure consistent dimension (truncate or pad if necessary)
        expected_dimension = DIMENSION
        if len(embedding_list) > expected_dimension:
            embedding_list = embedding_list[:expected_dimension]
        elif len(embedding_list) < expected_dimension:
            embedding_list += [0.0] * (expected_dimension - len(embedding_list))
        return embedding_list
    except Exception as e:
        logger.error(f"Error generating embedding: {str(e)}")
        return None


def classify_query_intent(query: str) -> dict:
    """
    Classify if a query requires university/course data lookup using LangChain, and what it is about.
    Args:
        query: User query string
    Returns:
        Dict with keys: requires_lookup (bool), target ('university', 'course', or 'both'), reason (str)
    """
    try:
        parser = PydanticOutputParser(pydantic_object=QueryIntent)
        prompt = PromptTemplate(
            template="""
            Determine if the following query is asking about universities, courses, or both, and whether it requires looking up specific data about universities or courses.
            
            Query: {query}
            
            If the query is asking about general information that doesn't require specific university or course data, classify it as not requiring lookup.
            
            Respond with:
            - requires_lookup: true/false
            - target: 'university', 'course', or 'both'
            - reason: short explanation
            
            {format_instructions}
            """,
            input_variables=["query"],
            partial_variables={"format_instructions": parser.get_format_instructions()},
        )
        chain = prompt | llm | parser
        result = chain.invoke({"query": query})
        return result.dict()
    except Exception as e:
        logger.error(f"Error classifying query intent: {str(e)}")
        return {"requires_lookup": False, "target": "unknown", "reason": str(e)}


def generate_chat_response(query: str, context: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate a response to a user query using LangChain.

    Args:
        query: User query string
        context: Optional context data from database

    Returns:
        Response string
    """
    try:
        # Format context if available
        if context and (context.get("universities") or context.get("courses")):
            # Format universities
            universities_text = ""
            if context.get("universities"):
                universities_text = "Universities:\n"
                for i, univ in enumerate(context["universities"], 1):
                    universities_text += f"{i}. {univ['university_name']} ({univ['country']})\n"
                    # Use campus_facilities as description if available, otherwise use a default
                    description = univ.get('campus_facilities', 'Information available upon request')
                    universities_text += f"   Description: {description}\n\n"

            # Format courses
            courses_text = ""
            if context.get("courses"):
                courses_text = "Courses:\n"
                for i, course in enumerate(context["courses"], 1):
                    courses_text += (
                        f"{i}. {course['name']} at {course['university_name']}\n"
                    )
                    courses_text += f"   Degree: {course['degree_type']}, Field: {course['field_of_study']}\n"
                    courses_text += f"   Duration: {course['duration']}, Fees: {course['tuition_fee']}\n"
                    courses_text += f"   Application Deadline: {course['application_deadline']}\n"
                    courses_text += f"   Description: {course['description']}\n\n"

            # Combine context
            context_text = f"{universities_text}\n{courses_text}"

            # Create prompt with context
            prompt = PromptTemplate(
                template="""
                You are a helpful assistant for university and course information.
                
                Based on the following information:
                
                {context}
                
                Please answer the user's query:
                {query}
                
                If the information provided doesn't fully answer the query, acknowledge that and provide what you can.
                """,
                input_variables=["context", "query"],
            )

            # Create and run the chain
            chain = prompt | llm | StrOutputParser()

            return chain.invoke({"context": context_text, "query": query})
        else:
            # Create prompt without context
            prompt = PromptTemplate(
                template="""
                You are a helpful assistant for university and course information.
                
                Please answer the user's query:
                {query}
                """,
                input_variables=["query"],
            )

            # Create and run the chain
            chain = prompt | llm | StrOutputParser()

            return chain.invoke({"query": query})

    except Exception as e:
        logger.error(f"Error generating chat response: {str(e)}")
        return "I'm sorry, I encountered an error while processing your query."
