import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

// Self-hosted fonts — no Google Fonts request, no FOUT flash.
import "@fontsource/bricolage-grotesque/700.css";
import "@fontsource/bricolage-grotesque/800.css";
import "@fontsource/hanken-grotesk/400.css";
import "@fontsource/hanken-grotesk/500.css";
import "@fontsource/hanken-grotesk/600.css";
import "@fontsource/azeret-mono/400.css";
import "@fontsource/azeret-mono/500.css";

import "./styles/theme.css";
import "./styles/workloads.css";
import App from "./App.jsx";

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <App />
  </StrictMode>
);
