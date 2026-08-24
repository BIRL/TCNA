import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import Select from "react-select";

interface Gene {
  gene_symbol: string;
  gene_name: string;
  tier: number | null;
  role_in_cancer: string;
}

interface MutationSample {
  sample_name: string;
  mutation_count: number;
}

interface CVResult {
  gene: string;
  num_samples: number;
  mean_mutation_count: number;
  std_mutation_count: number;
  cv_percent: number;
  samples: MutationSample[];
}

export default function MutationCosmicAnalysis() {
  const navigate = useNavigate();
  const [genes, setGenes] = useState<Gene[]>([]);
  const [selectedGene, setSelectedGene] = useState<Gene | null>(null);
  const [cvResult, setCvResult] = useState<CVResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch available genes on mount
  useEffect(() => {
    const fetchGenes = async () => {
      try {
        const res = await fetch("http://localhost:5001/api/cosmic-genes");
        const data = await res.json();
        setGenes(data.genes || []);
      } catch (err) {
        setError("Failed to load available genes");
        console.error(err);
      }
    };
    fetchGenes();
  }, []);

  // Handle gene analysis
  const handleAnalyze = async () => {
    if (!selectedGene) {
      setError("Please select a gene first");
      return;
    }

    setLoading(true);
    setError(null);
    setCvResult(null);

    try {
      const res = await fetch(
        `http://localhost:5001/api/cosmic-mutation-cv?gene=${selectedGene.gene_symbol}`
      );
      if (!res.ok) {
        throw new Error(`Server error: ${res.statusText}`);
      }
      const data = await res.json();
      setCvResult(data);
    } catch (err) {
      setError(`Failed to fetch CV data: ${err}`);
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  // Format gene options for react-select
  const geneOptions = genes.map((g) => ({
    value: g,
    label: `${g.gene_symbol} (${g.gene_name})`,
  }));

  return (
    <div style={{ padding: "20px", maxWidth: "1200px", margin: "0 auto" }}>
      <h1 style={{ color: "#003d82", marginBottom: "20px" }}>
        COSMIC Somatic Mutation CV Analysis
      </h1>

      {/* Gene Selection */}
      <div style={{ marginBottom: "30px" }}>
        <label style={{ display: "block", marginBottom: "10px", fontWeight: "bold" }}>
          Select a Cancer Gene:
        </label>
        <Select
          options={geneOptions}
          value={
            selectedGene
              ? {
                  value: selectedGene,
                  label: `${selectedGene.gene_symbol} (${selectedGene.gene_name})`,
                }
              : null
          }
          onChange={(opt) => opt && setSelectedGene(opt.value)}
          isSearchable
          isClearable
          styles={{
            control: (base) => ({
              ...base,
              minHeight: "40px",
              fontSize: "14px",
            }),
          }}
        />
      </div>

      {/* Analyze Button */}
      <Button
        onClick={handleAnalyze}
        disabled={!selectedGene || loading}
        style={{
          marginBottom: "20px",
          padding: "10px 20px",
          backgroundColor: "#003d82",
          color: "white",
          cursor: loading ? "not-allowed" : "pointer",
        }}
      >
        {loading ? "Analyzing..." : "Analyze Mutation CV"}
      </Button>

      {/* Error Display */}
      {error && (
        <div
          style={{
            padding: "15px",
            backgroundColor: "#fee",
            color: "#c00",
            borderRadius: "4px",
            marginBottom: "20px",
          }}
        >
          {error}
        </div>
      )}

      {/* Results Display */}
      {cvResult && (
        <div style={{ backgroundColor: "#f5f5f5", padding: "20px", borderRadius: "8px" }}>
          {/* Summary Stats */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
              gap: "20px",
              marginBottom: "30px",
            }}
          >
            <div
              style={{
                backgroundColor: "white",
                padding: "15px",
                borderRadius: "6px",
                boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
              }}
            >
              <div style={{ fontSize: "12px", color: "#666", marginBottom: "5px" }}>Gene</div>
              <div style={{ fontSize: "24px", fontWeight: "bold", color: "#003d82" }}>
                {cvResult.gene}
              </div>
            </div>

            <div
              style={{
                backgroundColor: "white",
                padding: "15px",
                borderRadius: "6px",
                boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
              }}
            >
              <div style={{ fontSize: "12px", color: "#666", marginBottom: "5px" }}>
                Coefficient of Variation (%)
              </div>
              <div style={{ fontSize: "24px", fontWeight: "bold", color: "#c00" }}>
                {cvResult.cv_percent.toFixed(2)}%
              </div>
            </div>

            <div
              style={{
                backgroundColor: "white",
                padding: "15px",
                borderRadius: "6px",
                boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
              }}
            >
              <div style={{ fontSize: "12px", color: "#666", marginBottom: "5px" }}>
                Samples
              </div>
              <div style={{ fontSize: "24px", fontWeight: "bold", color: "#003d82" }}>
                {cvResult.num_samples}
              </div>
            </div>

            <div
              style={{
                backgroundColor: "white",
                padding: "15px",
                borderRadius: "6px",
                boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
              }}
            >
              <div style={{ fontSize: "12px", color: "#666", marginBottom: "5px" }}>
                Mean Mutations/Sample
              </div>
              <div style={{ fontSize: "24px", fontWeight: "bold", color: "#003d82" }}>
                {cvResult.mean_mutation_count.toFixed(2)}
              </div>
            </div>
          </div>

          {/* Chart */}
          <div style={{ marginBottom: "30px", backgroundColor: "white", padding: "20px", borderRadius: "6px" }}>
            <h2 style={{ marginBottom: "15px", color: "#003d82" }}>
              Mutation Count Distribution (Top 20 Samples)
            </h2>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={cvResult.samples.slice(0, 20)}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="sample_name"
                  angle={-45}
                  textAnchor="end"
                  height={100}
                  interval={0}
                  tick={{ fontSize: 12 }}
                />
                <YAxis label={{ value: "Mutation Count", angle: -90, position: "insideLeft" }} />
                <Tooltip />
                <Bar dataKey="mutation_count" fill="#003d82" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Full Data Table */}
          <div style={{ backgroundColor: "white", padding: "20px", borderRadius: "6px" }}>
            <h2 style={{ marginBottom: "15px", color: "#003d82" }}>All Samples</h2>
            <div style={{ overflowX: "auto", maxHeight: "400px", overflowY: "auto" }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: "14px",
                }}
              >
                <thead>
                  <tr style={{ backgroundColor: "#e8f0f7", borderBottom: "2px solid #003d82" }}>
                    <th style={{ padding: "10px", textAlign: "left" }}>Sample Name</th>
                    <th style={{ padding: "10px", textAlign: "center" }}>Mutation Count</th>
                  </tr>
                </thead>
                <tbody>
                  {cvResult.samples.map((s, i) => (
                    <tr
                      key={i}
                      style={{
                        borderBottom: "1px solid #ddd",
                        backgroundColor: i % 2 === 0 ? "#f9f9f9" : "white",
                      }}
                    >
                      <td style={{ padding: "10px" }}>{s.sample_name}</td>
                      <td style={{ padding: "10px", textAlign: "center" }}>{s.mutation_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* No Results Yet */}
      {!cvResult && !loading && selectedGene && (
        <div style={{ color: "#666", fontStyle: "italic" }}>
          Click "Analyze Mutation CV" to see results.
        </div>
      )}

      {/* Back Button */}
      <Button
        onClick={() => navigate(-1)}
        style={{
          marginTop: "30px",
          padding: "10px 20px",
          backgroundColor: "#666",
          color: "white",
          cursor: "pointer",
        }}
      >
        ← Back
      </Button>
    </div>
  );
}
