import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import axios from 'axios';

// Set Global API Key for Axios
axios.defaults.headers.common['x-api-key'] = "telescope-secret-key-change-me";

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
