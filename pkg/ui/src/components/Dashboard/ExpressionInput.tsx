import { Input, Paper, Text, Stack, Box } from "@mantine/core";
import styles from "./expressionInput.module.css";
import { useEffect, useState, useRef } from "react";

interface LabelValuesProps {
    Services: string[];
    Levels: string[];
}

const ExpressionInput = () => {
    const [expression, setExpression] = useState("");
    const [data, setData] = useState<string[]>([
        "service",
        "level",
    ]);
    const [showSuggestions, setShowSuggestions] = useState(false);
    const [filteredSuggestions, setFilteredSuggestions] = useState<string[]>([]);
    const [activeSuggestionIndex, setActiveSuggestionIndex] = useState(-1);
    const inputRef = useRef<HTMLInputElement>(null);
    const suggestionsRef = useRef<HTMLDivElement>(null);

    const [labelDescriptionMap, setLabelDescriptionMap] = useState<Record<string, string>>({
        "service": "an expression to filter logs by service name",
        "level": "an expression to filter logs by log level (e.g., error, warning, info)",
    });

    const filterSuggestions = (inputValue: string) => {
        if (!inputValue.trim()) {
            setFilteredSuggestions([]);
            setShowSuggestions(false);
            return;
        }

        const tokens = inputValue.split(/[\s=]+/);
        const lastToken = tokens[tokens.length - 1].toLowerCase();

        if (lastToken === "") {
            setFilteredSuggestions([]);
            setShowSuggestions(false);
            return;
        }

        const filtered = data.filter(option => 
            option.toLowerCase().includes(lastToken)
        );

        setFilteredSuggestions(filtered);
        setShowSuggestions(filtered.length > 0);
        setActiveSuggestionIndex(-1);
    };

    const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const value = event.target.value;
        setExpression(value);
        filterSuggestions(value);
    };

    const handleSuggestionClick = (suggestion: string) => {
        const tokens = expression.split(/[\s=]+/);
        const lastToken = tokens[tokens.length - 1];
        const beforeLastToken = expression.substring(0, expression.lastIndexOf(lastToken));
        
        const newExpression = beforeLastToken + suggestion;
        setExpression(newExpression);
        setShowSuggestions(false);
        setActiveSuggestionIndex(-1);
        
        // Focus back to input
        if (inputRef.current) {
            inputRef.current.focus();
        }
    };

    const handleExpressionSubmit = async () => {
        const trimmedExpression = expression.trim();
        if (trimmedExpression === "") {
            return;
        }
        
        setShowSuggestions(false);
        
        try {
            const response = await fetch(`http://localhost:8080/query?expression=${trimmedExpression}`, {
                method: "GET",
                headers: {
                    "Content-Type": "application/json",
                },
            });
            
            if (!response.ok) {
                console.error("Failed to submit expression:", response.statusText);
                return;
            }
            
            const data = await response.json();
            console.log("Expression submitted successfully:", data);
        } catch (error) {
            console.error("Error submitting expression:", error);
        }
    };

    const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
        if (!showSuggestions) {
            if (event.key === "Enter") {
                event.preventDefault();
                handleExpressionSubmit();
            }
            return;
        }

        switch (event.key) {
            case "ArrowDown":
                event.preventDefault();
                setActiveSuggestionIndex(prev => 
                    prev < filteredSuggestions.length - 1 ? prev + 1 : 0
                );
                break;
            case "ArrowUp":
                event.preventDefault();
                setActiveSuggestionIndex(prev => 
                    prev > 0 ? prev - 1 : filteredSuggestions.length - 1
                );
                break;
            case "Enter":
                event.preventDefault();
                if (activeSuggestionIndex >= 0 && activeSuggestionIndex < filteredSuggestions.length) {
                    handleSuggestionClick(filteredSuggestions[activeSuggestionIndex]);
                } else {
                    handleExpressionSubmit();
                }
                break;
            case "Escape":
                setShowSuggestions(false);
                setActiveSuggestionIndex(-1);
                break;
        }
    };

    const handleInputFocus = () => {
        if (expression.trim()) {
            filterSuggestions(expression);
        }
    };

    const handleInputBlur = (event: React.FocusEvent<HTMLInputElement>) => {
        setTimeout(() => {
            if (!suggestionsRef.current?.contains(document.activeElement)) {
                setShowSuggestions(false);
                setActiveSuggestionIndex(-1);
            }
        }, 150);
    };

    useEffect(() => {
        const fetchLabelValues = async () => {
            try {
                const response = await fetch("http://localhost:8080/labels");
                if (!response.ok) {
                    throw new Error("Failed to fetch label values");
                }
                const data: LabelValuesProps = await response.json();
                console.log("Fetched label values:", data);

                setData([
                    "service",
                    "level",
                    ...data.Services,
                    ...data.Levels,
                ]);

                let newLabelDescriptionMap: Record<string, string> = {
                    "service": "an expression to filter logs by service name",
                    "level": "an expression to filter logs by log level (e.g., error, warning, info)",
                };

                data.Services.forEach((service: string) => {
                    newLabelDescriptionMap[service] = `Filter logs by service: ${service}`;
                });
                data.Levels.forEach((level: string) => {
                    newLabelDescriptionMap[level] = `Filter logs by log level: ${level}`;
                });

                setLabelDescriptionMap(newLabelDescriptionMap);
            } catch (error) {
                console.error("Error fetching label values:", error);
            }
        };

        fetchLabelValues();
    }, []);

    return (
        <Box className={styles.container}>
            <h1 className={styles.title}>Log Expression Input</h1>
            <Text size="sm" c="dimmed" mb={8}>
                Enter your log expression below. Use labels like <code>service</code> and <code>level</code> to filter logs.
            </Text>
            <Input
                ref={inputRef}
                placeholder="Enter expression"
                about="Enter your expression here, e.g., service=web AND level=error"
                value={expression}
                onChange={handleInputChange}
                onKeyDown={handleKeyDown}
                onFocus={handleInputFocus}
                onBlur={handleInputBlur}
                className={styles.expressionInput}
            />
            
            {showSuggestions && filteredSuggestions.length > 0 && (
                <Paper
                    ref={suggestionsRef}
                    shadow="md"
                    style={{
                        position: "absolute",
                        top: "100%",
                        left: 0,
                        right: 0,
                        zIndex: 1000,
                        maxHeight: "300px",
                        overflowY: "auto",
                        marginTop: "4px",
                    }}
                >
                    <Stack gap={0}>
                        {filteredSuggestions.map((suggestion, index) => (
                            <Box
                                key={suggestion}
                                style={{
                                    padding: "12px 16px",
                                    cursor: "pointer",
                                    backgroundColor: index === activeSuggestionIndex ? "#f1f3f4" : "transparent",
                                    borderBottom: index < filteredSuggestions.length - 1 ? "1px solid #e9ecef" : "none",
                                }}
                                onMouseDown={(e) => e.preventDefault()} // Prevent blur
                                onClick={() => handleSuggestionClick(suggestion)}
                                onMouseEnter={() => setActiveSuggestionIndex(index)}
                                className={styles.suggestionItem}
                            >
                                <Text size="sm" fw={500}>
                                    {suggestion}
                                </Text>
                                <Text size="xs" c="dimmed" mt={2}>
                                    {labelDescriptionMap[suggestion] || "No description available"}
                                </Text>
                            </Box>
                        ))}
                    </Stack>
                </Paper>
            )}
        </Box>
    );
};

export default ExpressionInput;