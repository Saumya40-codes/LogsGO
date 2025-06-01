import type { LogsPayload } from "../../types/types";

const LogData = ({ logs }: { logs: LogsPayload[] }) => {
    return (
        <div className="log-data">
            <h2>Log Data</h2>
            {logs.length > 0 ? (
                <table>
                    <thead>
                        <tr>
                            <th>Service</th>
                            <th>Level</th>
                            <th>Message</th>
                            <th>Timestamp</th>
                        </tr>
                    </thead>
                    <tbody>
                        {logs.map((log, index) => (
                            <tr key={index}>
                                <td>{log.service}</td>
                                <td>{log.level}</td>
                                <td>{log.message}</td>
                                <td>{log.timestamp}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            ) : (
                <p>No logs available.</p>
            )}
        </div>
    );
}

export default LogData;