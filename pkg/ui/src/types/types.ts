export interface LogsPayload {
    Service: string;
    Level: string;
    Message: string;
    TimeStamp: string[];
    Count: number[];
}

export interface LabelValuesProps {
    Services: string[];
    Levels: string[];
}