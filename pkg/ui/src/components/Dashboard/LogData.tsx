import type { LogsPayload } from "../../types/types";
import { useState } from "react";
import { Table, ScrollArea } from "@mantine/core";
import classes from './TableScrollArea.module.css';
import styles from './logdata.module.css';
import cx from 'clsx';

const LogData = ({ logs }: { logs: LogsPayload[] }) => {

    const [scrolled, setScrolled] = useState(false);
    const formatLabels = (labels?: Record<string, string>) => {
        if (!labels || Object.keys(labels).length === 0) return "";
        return Object.entries(labels)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([key, value]) => `${key}=${value}`)
            .join(", ");
    };

    const rows = logs.map((log, index) => (
        <Table.Tr key={index}>
        <Table.Td>{log.Service}</Table.Td>
        <Table.Td>{log.Level}</Table.Td>
        <Table.Td>{formatLabels(log.Labels)}</Table.Td>
        <Table.Td>{log.Message}</Table.Td>
        <Table.Td>
            {log.Points.map((point, idx) => (
                <div key={idx}>
                    {point.Count} : {point.Timestamp}
                </div>
            ))}
        </Table.Td>
        </Table.Tr>
    ));

    return (
        <div className={styles.container}>
            <ScrollArea onScrollPositionChange={({ y }) => setScrolled(y !== 0)} className={styles.scrollarea}>
                <Table miw={700}>
                    <Table.Thead className={cx(classes.header, { [classes.scrolled]: scrolled })}>
                    <Table.Tr>
                        <Table.Th>Service</Table.Th>
                        <Table.Th>Level</Table.Th>
                        <Table.Th>Labels</Table.Th>
                        <Table.Th>Message</Table.Th>
                        <Table.Th>Occurrence(s): TimeStamp(s)</Table.Th>
                    </Table.Tr>
                    </Table.Thead>
                    <Table.Tbody>{rows}</Table.Tbody>
                </Table>
            </ScrollArea>
        </div>
    );
}

export default LogData;
