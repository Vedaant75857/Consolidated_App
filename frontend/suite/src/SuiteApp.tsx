import React, { Suspense, lazy, useEffect, useState, type ComponentType } from "react";

const LandingApp = lazy(() => import("./routes/LandingRoute"));
const Module1App = lazy(() => import("./routes/Module1Route"));
const Module2App = lazy(() => import("./routes/Module2Route"));
const Module3App = lazy(() => import("./routes/Module3Route"));

const ROUTES = [
  ["/", "Home", LandingApp],
  ["/stitcher/", "Data Stitcher", Module1App],
  ["/normalizer/", "Data Normalizer", Module2App],
  ["/summarizer/", "Spend Summarizer", Module3App],
] as const satisfies ReadonlyArray<readonly [string, string, ComponentType]>;

function normalizePath(pathname: string) {
  if (pathname === "/") return "/";
  const withSlash = pathname.endsWith("/") ? pathname : `${pathname}/`;
  return withSlash;
}

function navigate(path: string) {
  if (window.location.pathname === path) return;
  window.history.pushState({}, "", path);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function RouteError({ reset }: { reset: () => void }) {
  return (
    <main className="suite-error" role="alert">
      <h1>We couldn&apos;t load this workspace</h1>
      <p>Refresh the page or return to the suite home.</p>
      <div className="suite-error-actions">
        <button type="button" onClick={reset}>Try again</button>
        <button type="button" onClick={() => navigate("/")}>Go home</button>
      </div>
    </main>
  );
}

function UnknownRoute({ pathname }: { pathname: string }) {
  return (
    <main className="suite-error" role="status">
      <h1>That suite route does not exist</h1>
      <p><code>{pathname}</code> is not one of the available workspaces.</p>
      <button type="button" onClick={() => navigate("/")}>Go to suite home</button>
    </main>
  );
}

class RouteBoundary extends React.Component<
  { children: React.ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) { return { error }; }
  render() {
    return this.state.error
      ? <RouteError reset={() => this.setState({ error: null })} />
      : this.props.children;
  }
}

// The boundary keeps failures isolated without coupling the host to any
// module's provider or state implementation.
function LazyRoute({ Component }: { Component: ComponentType }) {
  return <RouteBoundary><Component /></RouteBoundary>;
}

export default function SuiteApp() {
  const [pathname, setPathname] = useState(() => normalizePath(window.location.pathname));
  useEffect(() => {
    const onPopState = () => setPathname(normalizePath(window.location.pathname));
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  const route = ROUTES.find(([path]) => path === pathname);
  const [, title, Component] = route ?? ROUTES[0];
  return (
    <div className={`suite-shell suite-route-${title.toLowerCase().replace(/\s+/g, "-")}`}>
      <header className="suite-nav" aria-label="Suite navigation">
        <a className="suite-brand" href="/" onClick={(event) => { event.preventDefault(); navigate("/"); }}>
          ProcIP Suite
        </a>
        <nav>
          {ROUTES.map(([path, label]) => (
            <a
              key={path}
              href={path}
              aria-current={pathname === path ? "page" : undefined}
              onClick={(event) => { event.preventDefault(); navigate(path); }}
            >
              {label}
            </a>
          ))}
        </nav>
      </header>
      <div className="suite-route" data-route={pathname}>
        {route ? (
          <Suspense fallback={<div className="suite-loading" role="status" aria-live="polite">Loading {title}…</div>}>
            <LazyRoute Component={Component} />
          </Suspense>
        ) : <UnknownRoute pathname={pathname} />}
      </div>
    </div>
  );
}
