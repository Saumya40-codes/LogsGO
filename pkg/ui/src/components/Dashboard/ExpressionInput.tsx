import { Input, Text, Box, Button } from "@mantine/core";
import styles from "./expressionInput.module.css";
import { useState, useRef } from "react";
import type { LogsPayload } from "../../types/types";
import LogData from "./LogData";
import SuggestionFilter from "./SuggestionFilter";

const ExpressionInput = () => {
    const [expression, setExpression] = useState("");
    const inputRef = useRef<HTMLInputElement>(null);
    const [logs, setLogs] = useState<LogsPayload[]>([]);

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

    return (
        <Box className={styles.container}>
            <div className={styles.inputContainer}>
                <h1 className={styles.title}>Log Expression Input</h1>
                <Text size="sm" c="dimmed" mb={8}>
                    Enter your log expression below. Use labels like <code>service</code> and <code>level</code> to filter logs.
                </Text>
                <div className={styles.inputWrapper}>
                    <div className={styles.input}>
                        <Input
                            ref={inputRef}
                            placeholder="Enter expression"
                            about="Enter your expression here, e.g., service=web AND level=error"
                            value={expression}
                            onChange={handleInputChange}
                            onKeyDown={handleKeyDown}
                            className={styles.expressionInput}
                        />
                        <SuggestionFilter expression={expression} setExpression={setExpression} />
                    </div>
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
