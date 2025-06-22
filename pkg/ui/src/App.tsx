import './App.css'
import '@mantine/core/styles.css';
import '@mantine/dates/styles.css';
import { MantineProvider } from '@mantine/core';
import Dashboard from './components/Dashboard/Dashboard';
import Navbar from './components/Navbar/Navbar';
import { createTheme } from '@mantine/core';

const theme = createTheme({
  fontFamily: 'Inter, sans-serif',
  fontFamilyMonospace: 'Courier New, monospace',
  headings: {
    fontFamily: 'Inter, sans-serif',
  },
});

function App() {
  return (
    <MantineProvider theme={theme}>
      <div className="App">
        <Navbar />
        <Dashboard />
      </div>
    </MantineProvider>
  )
}

export default App
