export interface LogsPayload {
    Service: string;
    Level: string;
    Message: string;
    Labels?: Record<string, string>;
    Points: Points[];
}

type Points = {
    Count: number;
    Timestamp: number;
};

export interface LabelValuesProps {
    Services: string[];
    Levels: string[];
    CustomLabels?: Record<string, string[]>;
}
