import { useState, useEffect, useCallback, useMemo } from "react";
import type { LabelValuesProps } from "../../types/types";
import { Stack, Button, Loader, Text } from "@mantine/core";
import styles from "./suggestionfilter.module.css";

interface SuggestionFilterProps {
  expression: string;
  setExpression: (value: string) => void;
  apiUrl?: string;
  maxSuggestions?: number;
}

interface LoadingState {
  isLoading: boolean;
  error: string | null;
  hasLoaded: boolean;
}

const SuggestionFilter = ({ 
  expression, 
  setExpression, 
  apiUrl = "http://localhost:8080/api/v1/labels",
  maxSuggestions = 10 
}: SuggestionFilterProps) => {
  const defaultLabels = useMemo(() => ({
    labels: ["service", "level"],
    descriptions: {
      "service": "an expression to filter logs by service name",
      "level": "an expression to filter logs by log level (e.g., error, warning, info)",
    }
  }), []);

  const [labelDescriptionMap, setLabelDescriptionMap] = useState<Record<string, string>>(
    defaultLabels.descriptions
  );
  const [allLabels, setAllLabels] = useState<string[]>(defaultLabels.labels);
  const [loadingState, setLoadingState] = useState<LoadingState>({
    isLoading: false,
    error: null,
    hasLoaded: false
  });

  const fetchLabelValues = useCallback(async () => {
    if (loadingState.hasLoaded) return;
    
    setLoadingState(prev => ({ ...prev, isLoading: true, error: null }));
    
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000);
      
      const response = await fetch(apiUrl, {
        signal: controller.signal,
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        }
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const data: LabelValuesProps = await response.json();
      
      if (!data || typeof data !== 'object') {
        throw new Error('Invalid response format');
      }
      
      const services = Array.isArray(data.Services) ? data.Services : [];
      const levels = Array.isArray(data.Levels) ? data.Levels : [];
      
      const newLabels = Array.from(new Set([
        ...defaultLabels.labels,
        ...services.filter(s => typeof s === 'string' && s.trim()),
        ...levels.filter(l => typeof l === 'string' && l.trim()),
      ]));
      
      const newDescriptionMap: Record<string, string> = {
        ...defaultLabels.descriptions
      };
      
      services.forEach((service: string) => {
        if (typeof service === 'string' && service.trim()) {
          newDescriptionMap[service] = `Filter logs by service: ${service}`;
        }
      });
      
      levels.forEach((level: string) => {
        if (typeof level === 'string' && level.trim()) {
          newDescriptionMap[level] = `Filter logs by log level: ${level}`;
        }
      });
      
      setAllLabels(newLabels);
      setLabelDescriptionMap(newDescriptionMap);
      setLoadingState({ isLoading: false, error: null, hasLoaded: true });
      
    } catch (error) {
      const errorMessage = error instanceof Error 
        ? error.name === 'AbortError' 
          ? 'Request timed out'
          : error.message
        : 'Failed to fetch label values';
      
      console.error("Error fetching label values:", error);
      setLoadingState({ isLoading: false, error: errorMessage, hasLoaded: false });
    }
  }, [apiUrl, defaultLabels, loadingState.hasLoaded]);

  useEffect(() => {
    fetchLabelValues();
  }, [fetchLabelValues]);

  const getCurrentTerm = useCallback((expr: string): string => {
    if (!expr?.trim()) return '';
    
    const parts = expr.match(/"[^"]*"|'[^']*'|[\w-]+|[=|&()]/g);
    if (!parts || parts.length === 0) return '';
    
    const lastPart = parts[parts.length - 1];
    
    if (['=', '&', '|', '(', ')'].includes(lastPart)) {
      return '';
    }
    
    return lastPart.replace(/^["']|["']$/g, '');
  }, []);

  const filteredSuggestions = useMemo(() => {
    const currentTerm = getCurrentTerm(expression);
    
    if (!currentTerm) return [];
    
    const filtered = allLabels
      .filter(label => 
        label.toLowerCase().includes(currentTerm.toLowerCase()) &&
        label !== currentTerm
      )
      .sort((a, b) => {
        const aStarts = a.toLowerCase().startsWith(currentTerm.toLowerCase());
        const bStarts = b.toLowerCase().startsWith(currentTerm.toLowerCase());
        
        if (aStarts && !bStarts) return -1;
        if (!aStarts && bStarts) return 1;
        
        return a.localeCompare(b);
      })
      .slice(0, maxSuggestions);
    
    return filtered;
  }, [expression, allLabels, getCurrentTerm, maxSuggestions]);

  const handleSuggestionClick = useCallback((selectedLabel: string) => {
    if (!expression?.trim()) {
      setExpression(selectedLabel);
      return;
    }
    
    const currentTerm = getCurrentTerm(expression);
    
    if (!currentTerm) {
      setExpression(expression + selectedLabel);
      return;
    }
    
    const lastIndex = expression.lastIndexOf(currentTerm);
    if (lastIndex !== -1) {
      const newExpression = 
        expression.substring(0, lastIndex) + 
        selectedLabel + 
        expression.substring(lastIndex + currentTerm.length);
      setExpression(newExpression);
    } else {
      setExpression(expression + selectedLabel);
    }
  }, [expression, setExpression, getCurrentTerm]);

  const handleRetry = useCallback(() => {
    setLoadingState({ isLoading: false, error: null, hasLoaded: false });
    fetchLabelValues();
  }, [fetchLabelValues]);

  if (!expression?.trim() || filteredSuggestions.length === 0) {
    return null;
  }

  return (
    <Stack align="stretch" className={styles.suggestion__filter}>
      {loadingState.isLoading && (
        <div className={styles.suggestion__entry}>
          <Loader size="sm" />
          <Text size="sm" c="dimmed" ml="xs">Loading suggestions...</Text>
        </div>
      )}
      
      {loadingState.error && (
        <div className={styles.suggestion__entry}>
          <Text size="sm" c="red">
            Error: {loadingState.error}
          </Text>
          <Button 
            variant="subtle" 
            size="xs" 
            onClick={handleRetry}
            mt="xs"
          >
            Retry
          </Button>
        </div>
      )}
      
      {filteredSuggestions.map((label) => (
        <div key={label}>
          <Button 
            variant="subtle" 
            color="blue" 
            className={styles.suggestion__entry} 
            onClick={() => handleSuggestionClick(label)}
            fullWidth
          >
            <div>
              <Text fw={500}>{label}</Text>
              <Text size="xs" c="dimmed">
                {labelDescriptionMap[label] || "No description available"}
              </Text>
            </div>
          </Button>
        </div>
      ))}
    </Stack>
  );
};

export default SuggestionFilter;