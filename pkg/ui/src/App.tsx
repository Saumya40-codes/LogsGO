import './App.css'
import '@mantine/core/styles.css';
import { MantineProvider } from '@mantine/core';

function App() {
  return (
    <MantineProvider>
      <div className="App">
        <h1>LogsGo</h1>
        <p>A standalone log ingestion and querying tool.</p>
      </div>
    </MantineProvider>
  )
}

export default App
