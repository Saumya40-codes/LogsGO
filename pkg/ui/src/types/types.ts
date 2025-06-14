export interface LogsPayload {
    Service: string;
    Level: string;
    Message: string;
    Timestamp: string;
    Count: number;
}

export interface LabelValuesProps {
    Services: string[];
    Levels: string[];
}