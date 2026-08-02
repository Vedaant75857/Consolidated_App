import App from "../../../module1/src/App";
import { ThemeProvider } from "../../../module1/src/components/common/ThemeProvider";
import "../../../module1/src/index.css";

// Module 1 and Module 2 intentionally share datastitcher_theme for continuity
// with their standalone apps; the suite treats that as its shared light/dark key.
export default function Module1Route() {
  return <ThemeProvider><App /></ThemeProvider>;
}
