import { Autocomplete } from "@mantine/core";
import styles from "./expressionInput.module.css";
import { useState } from "react";

const ExpressionInput = () => {
    const [expression, setExpression] = useState("");

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
            data={[]} // TODO: Store all possible expressions in a constant and use it here on startup
            maxDropdownHeight={400}
            className={styles.expressionInput}
            value={expression}
            onChange={handleExpressionChange}
            onClick={handleExpressionSubmit}
            onKeyDown={handleKeyDown}
        />
    )
}

export default ExpressionInput;