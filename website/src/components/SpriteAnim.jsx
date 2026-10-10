export function SpriteAnim({ src, frames = 8, size, duration = 1400, alt = "", style }) {
  const animName = `sprite-anim-${size}-${frames}`;
  const className = `sprite-anim-class-${size}-${frames}`;

  const keyframesStyle = `
    @keyframes ${animName} {
      from { background-position-x: 0px; }
      to { background-position-x: -${size * frames}px; }
    }
    @media (prefers-reduced-motion: reduce) {
      .${className} {
        animation: none !important;
        background-position-x: 0px !important;
      }
    }
  `;

  const defaultStyle = {
    display: "inline-block",
    width: size,
    height: size,
    backgroundImage: `url(${src})`,
    backgroundRepeat: "no-repeat",
    backgroundSize: `${size * frames}px ${size}px`,
    imageRendering: "pixelated",
    animationName: animName,
    animationDuration: `${duration}ms`,
    animationTimingFunction: `steps(${frames})`,
    animationIterationCount: "infinite",
  };

  return (
    <>
      <style>{keyframesStyle}</style>
      <span
        role="img"
        aria-label={alt}
        className={className}
        style={{ ...defaultStyle, ...style }}
      />
    </>
  );
}
