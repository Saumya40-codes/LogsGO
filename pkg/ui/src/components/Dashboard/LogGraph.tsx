import { useMemo } from "react";
import { Badge, Text } from "@mantine/core";
import type { LogsPayload } from "../../types/types";
import styles from "./loggraph.module.css";

interface LogGraphProps {
    logs: LogsPayload[];
    startTs: number;
    endTs: number;
}

const formatTimestamp = (ts: number) =>
    new Date(ts * 1000).toLocaleString([], {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
    });

const formatSeriesTitle = (log: LogsPayload) => `${log.Service} · ${log.Level}`;

const LogGraph = ({ logs, startTs, endTs }: LogGraphProps) => {
    const series = useMemo(
        () =>
            logs
                .map((log) => ({
                    ...log,
                    Points: [...log.Points].sort((a, b) => a.Timestamp - b.Timestamp),
                }))
                .filter((log) => log.Points.length > 0),
        [logs]
    );

    const maxCount = Math.max(
        ...series.flatMap((log) => log.Points.map((point) => point.Count)),
        1
    );

    return (
        <div className={styles.graphCard}>
            <div className={styles.graphHeader}>
                <div>
                    <Text fw={600}>Graph Panel</Text>
                    <Text size="sm" c="dimmed">
                        Selected range: {formatTimestamp(startTs)} to {formatTimestamp(endTs)}
                    </Text>
                </div>
                <Badge variant="light" color="orange">
                    {series.length} series
                </Badge>
            </div>

            {series.length === 0 ? (
                <div className={styles.emptyState}>
                    <Text size="sm" c="dimmed">
                        No range data returned for this query.
                    </Text>
                </div>
            ) : (
                <div className={styles.seriesList}>
                    {series.map((log) => (
                        <section key={`${log.Service}-${log.Level}-${log.Message}`} className={styles.seriesCard}>
                            <div className={styles.seriesHeader}>
                                <div>
                                    <Text fw={600} className={styles.seriesTitle}>
                                        {formatSeriesTitle(log)}
                                    </Text>
                                    <Text size="sm" c="dimmed" className={styles.seriesMessage}>
                                        {log.Message}
                                    </Text>
                                </div>
                                <Badge variant="light" color="gray">
                                    {log.Points.length} point{log.Points.length === 1 ? "" : "s"}
                                </Badge>
                            </div>

                            <div className={styles.pointsList}>
                                {log.Points.map((point) => {
                                    const width = Math.max((point.Count / maxCount) * 100, 12);

                                    return (
                                        <div key={`${point.Timestamp}-${point.Count}`} className={styles.pointRow}>
                                            <div className={styles.pointMeta}>
                                                <Text size="sm" fw={500} className={styles.pointCount}>
                                                    {point.Count}
                                                </Text>
                                                <Text size="xs" c="dimmed">
                                                    {formatTimestamp(point.Timestamp)}
                                                </Text>
                                            </div>
                                            <div className={styles.barTrack} aria-hidden="true">
                                                <div
                                                    className={styles.barFill}
                                                    style={{ width: `${width}%` }}
                                                />
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        </section>
                    ))}
                </div>
            )}
        </div>
    );
};

export default LogGraph;
