export interface LogsPayload {
    service: string;
    level: string;
    message: string;
    timestamp: string;
}

export interface LabelValuesProps {
    Services: string[];
    Levels: string[];
}