import App from "../../../module3/src/App";
import { ThemeProvider } from "../../../module3/src/theme/ThemeProvider";
import "../../../module3/src/index.css";

export default function Module3Route() {
  return <ThemeProvider><App /></ThemeProvider>;
}
