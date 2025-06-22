import { Input, Text, Box, Button, TextInput, Tooltip, Center } from "@mantine/core";
import { DateTimePicker } from '@mantine/dates';
import styles from "./expressionInput.module.css";
import { useState, useRef } from "react";
import type { LogsPayload } from "../../types/types";
import LogData from "./LogData";
import SuggestionFilter from "./SuggestionFilter";
import { IconInfoCircle } from '@tabler/icons-react';

const ExpressionInput = () => {
    const [expression, setExpression] = useState("");
    const inputRef = useRef<HTMLInputElement>(null);
    const [logs, setLogs] = useState<LogsPayload[]>([]);
    const [startTs, setStartTs] = useState<number>(0);
    const [endTs, setEndTs] = useState<number>(0);
    const [resolution, setResolution] = useState<string>("15m");

    const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        const value = event.target.value;
        setExpression(value);
    };

    const handleExpressionSubmit = async () => {
        const trimmedExpression = expression.trim();
        if (trimmedExpression === "") {
            return;
        }
        const baseUrl = "http://localhost:8080/api/v1/query"; // TODO: Make this dynamic
        const params = new URLSearchParams();
        params.set("expression", trimmedExpression)
        params.set("start", (startTs).toString())
        params.set("end", (endTs).toString())
        params.set("resolution", resolution);

        const queryUrl = `${baseUrl}?${params.toString()}`;

        try {
            const response = await fetch(queryUrl, {
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

    const getUnixTime = (value: string | null) : number => {
        if (value === null) {
            return -1
        }

        const unixTime = Math.floor(new Date(value.replace(" ", "T")).getTime() / 1000);
        
        return unixTime
    }

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
                        <div className={styles.rangeSelector}>
                            <DateTimePicker 
                                withSeconds 
                                clearable 
                                label="Pick start time for query evaluation" 
                                placeholder="Pick start time" 
                                onChange={(value:string | null)=> {
                                    if(value === null) {
                                        setStartTs(0)
                                        return
                                    }
                                    setStartTs(getUnixTime(value))
                                }}
                            />
                            <DateTimePicker 
                                withSeconds 
                                clearable 
                                label="Pick end time for query evaluation" 
                                placeholder="Pick end time" 
                                onChange={(value:string | null)=> {
                                    if(value === null) {
                                        setEndTs(0)
                                        return
                                    }
                                    setEndTs(getUnixTime(value))
                                }}
                            />

                            <div className={styles.resolutionSelector}>
                                <TextInput 
                                    disabled={startTs === 0 || endTs === 0}
                                    value={resolution}
                                    onChange={(e) => setResolution(e.target.value)}
                                    placeholder="15m, 1h, 1d, etc."
                                    label="Resolution (Step Interval(size) for range query)"
                                    className={styles.resolutionInput}
                                />
                                <Tooltip
                                    label="For e.g. if its 15m, its shows data at 1am, 1:15am, 1:30am, etc. If its 1h, it shows data at 1am, 2am, 3am, etc."
                                    position="top-end"
                                    withArrow
                                    transitionProps={{ transition: 'pop-bottom-right' }}
                                >
                                    <Center>
                                        <IconInfoCircle size={20} color="gray" />
                                    </Center>
                                </Tooltip>
                            </div>
                        </div>
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
