import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { loadRuntimeConfig } from "../../landing/src/runtimeConfig";
import SuiteApp from "./SuiteApp";
import "./suite.css";

loadRuntimeConfig().finally(() => {
  const root = document.getElementById("root");
  if (!root) throw new Error("Suite root element is missing");
  createRoot(root).render(
    <StrictMode>
      <SuiteApp />
    </StrictMode>,
  );
});
