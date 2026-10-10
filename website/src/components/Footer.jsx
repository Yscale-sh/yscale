export function Footer() {
  return (
    <footer className="site-footer">
      <div className="wrap footer-shell">
        <div className="footer-brand">
          <span className="signal-mark signal-mark-small" aria-hidden="true">
            <b>y</b><i /><i /><i />
          </span>
          <div>
            <strong>yscale.sh</strong>
            <p>Self-hosted burst compute for Kubernetes.</p>
          </div>
        </div>

        <nav className="footer-links" aria-label="Footer navigation">
          <div>
            <span>PRODUCT</span>
            <a href="/#planes">Networking</a>
            <a href="/#included">Included source</a>
            <a href="/#faq">Common questions</a>
          </div>
          <div>
            <span>SELF-HOST</span>
            <a href="/docs/quickstart.html">Quickstart</a>
            <a href="/docs/getting-started.html">Installation guide</a>
            <a href="/docs/license.html">License</a>
            <a href="/docs/commercial.html">Commercial terms</a>
          </div>
          <div>
            <span>ECOSYSTEM</span>
            <a href="https://kubagachi.com">Kubagachi</a>
            <a href="https://market.kubagachi.com">KubeKritter</a>
            <a href="mailto:yscale@unbelievablesite.com">Contact</a>
          </div>
        </nav>

        <div className="footer-base">
          <span>© 2026 Yscale</span>
          <span>You operate the services and pay providers directly.</span>
          <span>Source-available · self-hosted</span>
        </div>
      </div>
    </footer>
  );
}
