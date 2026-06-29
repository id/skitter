import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import 'allotment/dist/style.css'
import './index.css'
import '@/dashboard/i18n'
import App from './App.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
