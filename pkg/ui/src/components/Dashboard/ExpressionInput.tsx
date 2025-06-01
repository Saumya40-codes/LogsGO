import { Input, Text, Box, Button } from "@mantine/core";
import styles from "./expressionInput.module.css";
import { useEffect, useState, useRef } from "react";
import type { LogsPayload } from "../../types/types";
import LogData from "./LogData";

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
    const inputRef = useRef<HTMLInputElement>(null);
    const [logs, setLogs] = useState<LogsPayload[]>([]);

    const [labelDescriptionMap, setLabelDescriptionMap] = useState<Record<string, string>>({
        "service": "an expression to filter logs by service name",
        "level": "an expression to filter logs by log level (e.g., error, warning, info)",
    });

    const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const value = event.target.value;
        setExpression(value);
    };

    const handleExpressionSubmit = async () => {
        const trimmedExpression = expression.trim();
        if (trimmedExpression === "") {
            return;
        }
                
        try {
            const response = await fetch(`http://localhost:8080/api/v1/query?expression=${trimmedExpression}`, {
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
            setLogs(data || []);
            console.log("Logs fetched successfully:", data);
        } catch (error) {
            console.error("Error submitting expression:", error);
        }
    };

    const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
         if (event.key === "Enter") {
            handleExpressionSubmit();
        }
    };

    useEffect(() => {
        const fetchLabelValues = async () => {
            try {
                const response = await fetch("http://localhost:8080/api/v1/labels");
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
            <div className={styles.inputContainer}>
                <h1 className={styles.title}>Log Expression Input</h1>
                <Text size="sm" c="dimmed" mb={8}>
                    Enter your log expression below. Use labels like <code>service</code> and <code>level</code> to filter logs.
                </Text>
                <div className={styles.inputWrapper}>
                    <Input
                        ref={inputRef}
                        placeholder="Enter expression"
                        about="Enter your expression here, e.g., service=web AND level=error"
                        value={expression}
                        onChange={handleInputChange}
                        onKeyDown={handleKeyDown}
                        className={styles.expressionInput}
                    />
                    <Button
                        onClick={handleExpressionSubmit}
                        className={styles.submitButton}
                        disabled={!expression.trim()}
                    >
                        Execute
                    </Button>
                </div>
            </div>
            <div className={styles.logsContainer}>
                <LogData logs={logs} />
            </div>
        </Box>
    );
};

export default ExpressionInput;
