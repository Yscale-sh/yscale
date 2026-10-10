import { Link } from "../lib/router.jsx";

export function Nav() {
  return (
    <nav className="site-nav release-navigation" aria-label="Primary navigation">
      <div className="wrap nav-shell">
        <Link to="/" className="wordmark" aria-label="yscale.sh home">
          <span className="signal-mark" aria-hidden="true">
            <b>y</b><i /><i /><i />
          </span>
          <span>scale<em>.sh</em></span>
        </Link>

        <div className="nav-state" aria-label="Product status">
          <i aria-hidden="true" />
          <span>source-available</span>
        </div>

        <div className="nav-links">
          <a href="/#planes">networking</a>
          <a href="/#license">license</a>
          <a href="/docs/">docs</a>
          <a href="/docs/quickstart.html" className="nav-account">quickstart <span aria-hidden="true">↗</span></a>
        </div>
      </div>
    </nav>
  );
}
