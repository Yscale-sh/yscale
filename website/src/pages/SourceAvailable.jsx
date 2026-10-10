import { MotionConfig, motion } from "motion/react";
import { ClusterField } from "../components/ClusterField.jsx";
import { EASE, Reveal } from "../components/Reveal.jsx";

const heroRise = {
  hidden: { opacity: 0, y: 36 },
  show: (i) => ({ opacity: 1, y: 0, transition: { duration: 0.9, delay: 0.1 + i * 0.12, ease: EASE } }),
};

const INCLUDED = [
  ["01", "Compute and networking", "Central, the Cluster Connector and provider adapters provision burst nodes. The gateway connects them to your cluster network."],
  ["02", "Mesh coordination", "The fabric factory manages tenant isolation and Headscale/DERP coordination. You run the services and their supporting infrastructure."],
  ["03", "Application and business logic", "Account and tenant management, cloud cost estimates, operator tools, and the console are included in the same distribution."],
];

export function SourceAvailable() {
  return (
    <MotionConfig reducedMotion="user">
    <div className="source-release">
      <section className="source-hero wrap" aria-labelledby="source-title">
        <ClusterField />
        <motion.p className="eyebrow" variants={heroRise} initial="hidden" animate="show" custom={0}>SELF-HOSTED KUBERNETES BURST COMPUTE</motion.p>
        <h1 id="source-title"><motion.span className="hero-line" variants={heroRise} initial="hidden" animate="show" custom={1}>Add nodes from any cloud to your cluster</motion.span><br /> <motion.span className="hero-line hero-line-muted" variants={heroRise} initial="hidden" animate="show" custom={2}>when the workload demands it.</motion.span></h1>
        <div className="source-intro">
          <motion.div variants={heroRise} initial="hidden" animate="show" custom={3.5}>
            <p className="source-lede">Karpenter for any cloud.</p>
            <p>Give developers GPU access across supported clouds—or burst from your homelab—with your platform in control.</p>
            <div className="source-actions">
              <a className="btn-solid" href="/docs/quickstart.html">Follow the quickstart <span aria-hidden="true">↗</span></a>
              <a className="btn-ghost" href="/docs/getting-started.html">Read the installation guide <span aria-hidden="true">↗</span></a>
              <a className="btn-ghost" href="/docs/networking.html">Review networking requirements <span aria-hidden="true">↗</span></a>
            </div>
            <p className="source-status">Experimental. Provider support and network performance vary. <a href="/docs/configuration-status.html">Supported configurations.</a></p>
          </motion.div>
          <motion.div className="source-contract-motion" initial={{ opacity: 0, x: 32 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.9, delay: 0.55, ease: EASE }} whileHover={{ y: -6, rotate: -0.6 }}>
          <aside className="source-contract" aria-label="Deployment requirements">
            <span className="eyebrow">WHAT YOU OPERATE</span>
            <strong>Run Yscale on<br />your infrastructure.</strong>
            <dl>
              <div><dt>Cluster</dt><dd>Your existing Kubernetes environment</dd></div>
              <div><dt>Cloud</dt><dd>Your accounts and provider bills</dd></div>
              <div><dt>Services</dt><dd>Control plane, databases and mesh</dd></div>
              <div><dt>Source</dt><dd>Infrastructure and business logic</dd></div>
            </dl>
          </aside>
          </motion.div>
        </div>
      </section>

      <section className="source-network" id="planes" aria-labelledby="network-title">
        <div className="wrap">
          <Reveal className="source-section-heading"><p className="eyebrow">01 / NETWORKING</p><h2 id="network-title">Connect burst nodes<br />to your cluster network.</h2></Reveal>
          <ol className="source-flow" aria-label="Workload lifecycle">
            <Reveal as="li" delay={0.00}><span>01 / COORDINATION</span><strong>Set up Headscale</strong><p>Run your own tenant and operations coordinators, with a separate private callback path for the factory.</p></Reveal>
            <Reveal as="li" delay={0.12}><span>02 / ROUTING</span><strong>Connect the networks</strong><p>Burst-side Cilium reaches your existing cluster CNI through the encrypted mesh and gateway.</p></Reveal>
            <Reveal as="li" delay={0.24}><span>03 / VERIFICATION</span><strong>Check the workload path</strong><p>Verify routes, policy behavior and required private services before putting burst nodes to work.</p></Reveal>
          </ol>
          <p className="source-section-note">No Tailscale account or subscription is required. Tailscale client software still runs on infrastructure nodes; workload authors do not need a VPN app on their laptops. Your existing CNI stays in place, with routing configured and verified for your cluster. <a href="/docs/networking.html">Read the packet path and network limits.</a></p>
        </div>
      </section>

      <section className="wrap source-section" id="included" aria-labelledby="included-title">
        <Reveal className="source-section-heading"><p className="eyebrow">02 / INCLUDED SOURCE</p><h2 id="included-title">Application and business logic included.</h2></Reveal>
        <p className="source-section-note">The full-source distribution includes the services, console and business logic, with installation guides and configuration examples.</p>
        <div className="source-included">
          {INCLUDED.map(([number, title, body], i) => <Reveal key={number} delay={i * 0.1}><span>{number}</span><h3>{title}</h3><p>{body}</p></Reveal>)}
        </div>
        <p>You self-host your applications on infrastructure you manage and pay your cloud providers directly.</p>
      </section>

      <section className="source-usecases" aria-labelledby="usecases-title">
        <div className="wrap source-section">
          <Reveal className="source-section-heading"><p className="eyebrow">03 / WORKLOADS</p><h2 id="usecases-title">Run batch jobs and workers<br />on temporary capacity.</h2></Reveal>
          <div className="source-case-grid">
            <Reveal delay={0.0}><h3>Batch jobs and builds</h3><p>Move containerized processing, selected CI jobs, or test suites onto temporary capacity. Keep orchestration and durable outputs under your control.</p></Reveal>
            <Reveal delay={0.1}><h3>Data preparation and GPU jobs</h3><p>Run CPU preprocessing or GPU workloads on supported infrastructure. Use pre-seeded Linode model volumes or copy inputs from S3-compatible storage, including R2.</p></Reveal>
            <Reveal delay={0.2}><h3>Media processing</h3><p>yscale-media prioritizes chunks near the playhead and uses worker ranking and encoder-family affinity. Its burst-worker templates are a starting point for your integration; cloud scaling still needs configuration.</p></Reveal>
          </div>
          <a className="source-text-link" href="/docs/articles/burst-where-it-makes-sense.html">Read the use cases and measured storage results <span aria-hidden="true">→</span></a>
        </div>
      </section>

      <section className="wrap source-section" id="license" aria-labelledby="license-title">
        <Reveal className="source-section-heading"><p className="eyebrow">04 / LICENSING</p><h2 id="license-title">Self-hosting rights<br />and commercial licensing.</h2></Reveal>
        <p className="source-section-note">Yscale is source-available, not OSI open source. The license sets out the permissions and conditions.</p>
        <div className="source-license-grid">
          <Reveal delay={0.0}><h3>Personal and internal business use</h3><p>Self-hosting for yourself or your organization is free of license fees, regardless of company size or revenue. Running your own applications is different from selling Yscale functionality.</p></Reveal>
          <Reveal delay={0.1}><h3>Resale and third-party hosting</h3><p>Reselling Yscale, offering it as a managed service, or brokering its compute functionality to third parties requires a separate paid written license.</p></Reveal>
        </div>
        <p>You pay providers directly for compute, storage, networking and coordination servers. Software budgets are not provider-enforced spending caps.</p>
        <div className="source-actions"><a className="btn-ghost" href="/docs/license.html">Read the license</a><a className="source-text-link" href="/docs/commercial.html">Commercial terms →</a></div>
      </section>

      <section className="wrap source-section source-faq" id="faq" aria-labelledby="faq-title">
        <Reveal className="source-section-heading"><p className="eyebrow">05 / COMMON QUESTIONS</p><h2 id="faq-title">Before you install.</h2></Reveal>
        <details><summary>Does Yscale host the service for me?</summary><p>No. You operate central, the factory, connector, gateway, databases and mesh on infrastructure you manage. Self-hosting does not require a Yscale-hosted account.</p></details>
        <details><summary>Do I need a Tailscale account?</summary><p>No. Use your own Headscale coordinators for the workload mesh and the separate operations mesh. Infrastructure nodes use the Tailscale client software. Workload authors use Kubernetes access, without a VPN app on their laptops. <a href="/docs/factory-environment.html#self-hosted-ops-setup">Configure the operations mesh.</a></p></details>
        <details><summary>What do operators need to set up?</summary><p>Deploy the services, databases and mesh coordinators. Build your node images and configure provider credentials, quotas, TLS and private routing. The factory uses Linode for persistent coordination boxes, even when workers run elsewhere. <a href="/docs/self-hosted-mesh.html">Review the operator prerequisites.</a></p></details>
        <details><summary>Which clouds and GPUs are supported?</summary><p>The source includes Fly CPU, Linode and AWS CPU/GPU, and Azure and GCP CPU paths. Azure and GCP GPU support is outside this release scope. Verify your specific region, image, instance shape and cluster before relying on it. <a href="/docs/configuration-status.html">Check configuration status.</a></p></details>
        <details><summary>Is Yscale open source?</summary><p>Yscale is source-available. Restrictions on resale and third-party hosting do not meet the Open Source Definition. Read the license for the permissions and conditions that apply. <a href="https://opensource.org/osd">Read the Open Source Definition.</a></p></details>
      </section>

      <section className="source-close"><Reveal className="wrap"><p className="eyebrow">INSTALLATION</p><h2>Plan your Yscale setup.</h2><p>Review the infrastructure requirements and try the no-cloud smoke test. Then choose a small workload to verify provisioning, execution and cleanup on your installation.</p><a className="btn-solid" href="/docs/getting-started.html">Read the installation guide <span aria-hidden="true">↗</span></a></Reveal></section>
    </div>
    </MotionConfig>
  );
}
