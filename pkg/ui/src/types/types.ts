export interface LogsPayload {
    Service: string;
    Level: string;
    Message: string;
    Series: Series[];
}

type Series = {
    Count: number;
    Timestamp: string;
};

export interface LabelValuesProps {
    Services: string[];
    Levels: string[];
}