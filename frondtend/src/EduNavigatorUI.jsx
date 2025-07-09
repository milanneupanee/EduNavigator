import React, { useState } from "react";
import axios from "axios";
import { BookOpen, School, Search, LoaderCircle } from "lucide-react";

export default function EduNavigatorUI() {
  const [query, setQuery] = useState("");
  const [searchType, setSearchType] = useState("all");
  const [limit, setLimit] = useState(5);
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSearch = async () => {
    setLoading(true);
    setError(null);
    setResults(null);
    try {
      const response = await axios.get("/search", {
        params: {
          query,
          limit,
          search_type: searchType,
        },
      });
      setResults(response.data);
    } catch (err) {
      setError(err.response?.data?.detail || "An error occurred");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto p-6">
      <h1 className="text-3xl font-bold mb-6 flex items-center gap-2">
        <BookOpen className="text-blue-600" /> EduNavigator
      </h1>

      <div className="space-y-4">
        <div className="flex items-center gap-2">
          <Search className="text-gray-500" />
          <input
            type="text"
            placeholder="Enter your search query..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="w-full p-2 border rounded"
          />
        </div>

        <div className="flex gap-4 items-center">
          <select
            value={searchType}
            onChange={(e) => setSearchType(e.target.value)}
            className="p-2 border rounded"
          >
            <option value="all">All</option>
            <option value="universities">Universities</option>
            <option value="courses">Courses</option>
          </select>

          <input
            type="number"
            value={limit}
            onChange={(e) => setLimit(parseInt(e.target.value))}
            className="p-2 border rounded w-24"
            min={1}
            max={50}
          />

          <button
            onClick={handleSearch}
            className="bg-blue-600 text-white px-4 py-2 rounded flex items-center gap-2"
          >
            {loading ? (
              <>
                <LoaderCircle className="animate-spin" /> Searching...
              </>
            ) : (
              <>
                <Search /> Search
              </>
            )}
          </button>
        </div>

        {error && <p className="text-red-600">{error}</p>}

        {results && (
          <div className="mt-6 space-y-8">
            {results.universities.length > 0 && (
              <div>
                <h2 className="text-xl font-semibold flex items-center gap-2">
                  <School className="text-green-600" /> Universities
                </h2>
                <ul className="mt-2 space-y-2">
                  {results.universities.map((u) => (
                    <li
                      key={u.id}
                      className="border p-4 rounded bg-white shadow"
                    >
                      <h3 className="font-bold">{u.name} ({u.country})</h3>
                      <p>{u.description}</p>
                      <p className="text-sm text-gray-600">Similarity: {u.similarity_score.toFixed(2)}</p>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {results.courses.length > 0 && (
              <div>
                <h2 className="text-xl font-semibold flex items-center gap-2">
                  <BookOpen className="text-purple-600" /> Courses
                </h2>
                <ul className="mt-2 space-y-2">
                  {results.courses.map((c) => (
                    <li
                      key={c.id}
                      className="border p-4 rounded bg-white shadow"
                    >
                      <h3 className="font-bold">{c.name} - {c.university_name}</h3>
                      <p>{c.description}</p>
                      <div className="text-sm text-gray-700 space-y-1">
                        <p>Field: {c.field_of_study}</p>
                        <p>Degree: {c.degree_type}</p>
                        {c.starting_date && <p>Start: {c.starting_date}</p>}
                        {c.duration && <p>Duration: {c.duration}</p>}
                        {c.fee_structure && <p>Fee: {c.fee_structure}</p>}
                        {c.language_of_study && <p>Language: {c.language_of_study}</p>}
                        <p className="text-gray-600">Similarity: {c.similarity_score.toFixed(2)}</p>
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
