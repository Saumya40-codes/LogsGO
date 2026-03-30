export interface LogsPayload {
    Service: string;
    Level: string;
    Message: string;
    Points: Points[];
}

type Points = {
    Count: number;
    Timestamp: string;
};

export interface LabelValuesProps {
    Services: string[];
    Levels: string[];
}