import App from "../../../module2/src/App";
import { ThemeProvider } from "../../../module2/src/components/common/ThemeProvider";
import "../../../module2/src/index.css";

export default function Module2Route() {
  return <ThemeProvider><App /></ThemeProvider>;
}
