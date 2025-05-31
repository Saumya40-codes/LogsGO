import { Autocomplete } from "@mantine/core";
import styles from "./expressionInput.module.css";
import { useEffect, useState } from "react";

const ExpressionInput = () => {
    const [expression, setExpression] = useState("");
    const [data, setData] = useState<string[]>([
        "service",
        "level",
    ])

    const labelDescriptionMap: Record<string, string> = {
        "service": "an expression to filter logs by service name",
        "level": "an expression to filter logs by log level (e.g., error, warning, info)",
    };

    const handleExpressionChange = (value: string) => {
        setExpression(value);
    }

    const handleExpressionSubmit = async() => {
        const trimmedExpression = expression.trim();
        if (trimmedExpression === "") {
            return;
        }
        const response = await fetch(`http://localhost:8080/query?expression=${trimmedExpression}`, {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
            },
        });
        setExpression("");
        if (!response.ok) {
            console.error("Failed to submit expression:", response.statusText);
            return;
        }
        const data = await response.json();
        console.log("Expression submitted successfully:", data);
    }

    const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
        if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            handleExpressionSubmit();
        }
    }

    return (
        <Autocomplete
            placeholder="Enter expression"
            label="Expression"
            description="Get started with: your_service_name"
            data={data}// TODO: Store all possible expressions in a constant and use it here on startup
            maxDropdownHeight={400}
            className={styles.expressionInput}
            value={expression}
            onChange={handleExpressionChange}
            onClick={handleExpressionSubmit}
            onKeyDown={handleKeyDown}
            renderOption={({ option }) => {
            return (
                <div style={{ display: "flex", flexDirection: "column" }}>
                <span>{option.value}</span>
                <span style={{ fontSize: 12, color: "#00000" }}>
                    {labelDescriptionMap[option.value] || "No description available"}
                </span>
                </div>
            );
            }}
        />
    )
}

export default ExpressionInput;