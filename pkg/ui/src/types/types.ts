export interface LogsPayload {
    service: string;
    level: string;
    message: string;
    timestamp: string;
    count: number;
}

export interface LabelValuesProps {
    Services: string[];
    Levels: string[];
}