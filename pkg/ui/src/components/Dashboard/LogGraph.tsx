import { useMemo } from "react";
import { Badge, Text } from "@mantine/core";
import type { LogsPayload } from "../../types/types";
import styles from "./loggraph.module.css";

interface LogGraphProps {
    logs: LogsPayload[];
    startTs: number;
    endTs: number;
}

type ChartPoint = {
    count: number;
    timestamp: number;
    x: number;
    y: number;
};

const CHART_WIDTH = 720;
const CHART_HEIGHT = 240;
const CHART_PADDING = {
    top: 28,
    right: 24,
    bottom: 44,
    left: 44,
};

const formatTimestamp = (ts: number) =>
    new Date(ts * 1000).toLocaleString([], {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
    });

const formatSeriesTitle = (log: LogsPayload) => `${log.Service} · ${log.Level}`;

const buildPath = (points: ChartPoint[]) =>
    points
        .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
        .join(" ");

const buildChartPoints = (timestamps: number[], counts: number[]): ChartPoint[] => {
    if (timestamps.length === 0 || counts.length === 0) {
        return [];
    }

    const innerWidth = CHART_WIDTH - CHART_PADDING.left - CHART_PADDING.right;
    const innerHeight = CHART_HEIGHT - CHART_PADDING.top - CHART_PADDING.bottom;
    const minTs = timestamps[0];
    const maxTs = timestamps[timestamps.length - 1];
    const tsSpan = Math.max(maxTs - minTs, 1);
    const maxCount = Math.max(...counts, 1);

    return timestamps.map((timestamp, index) => ({
        timestamp,
        count: counts[index],
        x: CHART_PADDING.left + ((timestamp - minTs) / tsSpan) * innerWidth,
        y: CHART_PADDING.top + innerHeight - (counts[index] / maxCount) * innerHeight,
    }));
};

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
                    {series.map((log) => {
                        const timestamps = log.Points.map((point) => point.Timestamp);
                        const counts = log.Points.map((point) => point.Count);
                        const chartPoints = buildChartPoints(timestamps, counts);
                        const path = buildPath(chartPoints);
                        const maxCount = Math.max(...counts, 1);

                        return (
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
                                        peak {maxCount}
                                    </Badge>
                                </div>

                                <div className={styles.chartWrap}>
                                    <svg
                                        viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`}
                                        role="img"
                                        aria-label={`Line graph for ${log.Service} ${log.Level}`}
                                        className={styles.chart}
                                    >
                                        <line
                                            x1={CHART_PADDING.left}
                                            y1={CHART_HEIGHT - CHART_PADDING.bottom}
                                            x2={CHART_WIDTH - CHART_PADDING.right}
                                            y2={CHART_HEIGHT - CHART_PADDING.bottom}
                                            className={styles.axisLine}
                                        />
                                        <line
                                            x1={CHART_PADDING.left}
                                            y1={CHART_PADDING.top}
                                            x2={CHART_PADDING.left}
                                            y2={CHART_HEIGHT - CHART_PADDING.bottom}
                                            className={styles.axisLine}
                                        />

                                        {[0, 0.5, 1].map((ratio) => {
                                            const y = CHART_PADDING.top + (CHART_HEIGHT - CHART_PADDING.top - CHART_PADDING.bottom) * ratio;
                                            const value = Math.round(maxCount * (1 - ratio));

                                            return (
                                                <g key={ratio}>
                                                    <line
                                                        x1={CHART_PADDING.left}
                                                        y1={y}
                                                        x2={CHART_WIDTH - CHART_PADDING.right}
                                                        y2={y}
                                                        className={styles.gridLine}
                                                    />
                                                    <text x={10} y={y + 4} className={styles.axisLabel}>
                                                        {value}
                                                    </text>
                                                </g>
                                            );
                                        })}

                                        {chartPoints.length > 1 && (
                                            <path d={path} className={styles.linePath} />
                                        )}

                                        {chartPoints.map((point) => (
                                            <g key={`${point.timestamp}-${point.count}`}>
                                                <circle cx={point.x} cy={point.y} r={4.5} className={styles.dataPoint} />
                                                <text x={point.x} y={point.y - 10} textAnchor="middle" className={styles.countLabel}>
                                                    {point.count}
                                                </text>
                                                <text
                                                    x={point.x}
                                                    y={CHART_HEIGHT - 14}
                                                    textAnchor="middle"
                                                    className={styles.timeLabel}
                                                >
                                                    {formatTimestamp(point.timestamp)}
                                                </text>
                                            </g>
                                        ))}
                                    </svg>
                                </div>
                            </section>
                        );
                    })}
                </div>
            )}
        </div>
    );
};

export default LogGraph;
