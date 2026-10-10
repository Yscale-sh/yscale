import { motion } from "motion/react";

// One scroll-reveal primitive for the page: a short rise and fade the first time
// an element enters the viewport. Motion owns these transforms; MotionConfig
// reducedMotion="user" (set on the page) drops the movement for people who ask.
export const EASE = [0.22, 1, 0.36, 1];

export function Reveal({ as = "div", delay = 0, y = 28, children, ...rest }) {
  const Tag = motion[as];
  return (
    <Tag
      initial={{ opacity: 0, y }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, amount: 0.2 }}
      transition={{ duration: 0.7, delay, ease: EASE }}
      {...rest}
    >
      {children}
    </Tag>
  );
}
