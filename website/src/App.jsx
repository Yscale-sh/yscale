import { useEffect, useState } from "react";
import { useRoute } from "./lib/router.jsx";
import { Nav } from "./components/Nav.jsx";
import { Footer } from "./components/Footer.jsx";
import { Unsubscribe } from "./pages/Unsubscribe.jsx";
import { Console } from "./pages/Console.jsx";
import { Account } from "./pages/Account.jsx";
import { Callback } from "./pages/Callback.jsx";
import { Workloads } from "./pages/Workloads.jsx";
import { SourceAvailable } from "./pages/SourceAvailable.jsx";
import "./styles/source-release.css";

function useIsMobile() {
  const [mobile, setMobile] = useState(
    () => typeof window !== "undefined" && window.matchMedia("(max-width: 880px)").matches
  );
  useEffect(() => {
    const mq = window.matchMedia("(max-width: 880px)");
    const on = () => setMobile(mq.matches);
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, []);
  return mobile;
}


function pageFor(path, isMobile) {
  switch (path) {
    case "/unsubscribe":
      return <Unsubscribe isMobile={isMobile} />;
    case "/open-source":
    case "/source-available":
      return <SourceAvailable />;
    case "/console":
      return <Console />;
    case "/account":
      return <Account />;
    case "/callback":
      return <Callback />;
    default:
      return <SourceAvailable />;
  }
}

export default function App() {
  const isMobile = useIsMobile();
  const [path] = useRoute();
  const page = pageFor(path, isMobile);

  if (path === "/workloads" || path.startsWith("/workloads/")) {
    return (
      <>
        <a className="skip-link" href="#main-content">skip to content</a>
        <Workloads path={path} />
      </>
    );
  }

  return (
    <>
      <a className="skip-link" href="#main-content">skip to content</a>
      <Nav isMobile={isMobile} accountRoute={path === "/account" || path === "/callback"} />
      <main id="main-content" tabIndex={-1}>{page}</main>
      <Footer isMobile={isMobile} accountRoute={path === "/account" || path === "/callback"} />
    </>
  );
}
