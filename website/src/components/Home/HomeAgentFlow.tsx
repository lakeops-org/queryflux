import {useEffect, useRef, useState, type ReactNode} from 'react';
import clsx from 'clsx';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './styles.module.css';

type Engine = {id: string; label: string; hint: string; logo: string};

const ENGINE_DEFS: Omit<Engine, 'logo'>[] = [
  {id: 'duckdb', label: 'DuckDB', hint: 'agent scratchpad'},
  {id: 'trino', label: 'Trino', hint: 'etl'},
  {id: 'starrocks', label: 'StarRocks', hint: 'BI'},
];

const PICK = 'duckdb';
const QUESTION = 'How many US events landed in the last 7 days?';

/** Hub (left-center) → engine rows. viewBox 0 0 100 180 */
const FAN_PATHS = [
  'M 6 90 C 46 90 46 30 94 30',
  'M 6 90 H 94',
  'M 6 90 C 46 90 46 150 94 150',
] as const;

/** Visible dialect rewrite: Trino date_add → DuckDB interval arithmetic. */
const SQL_SOURCE = `SELECT date_trunc('day', ts) AS d,
       count(*) AS n
FROM events
WHERE country = 'US'
  AND ts >= date_add('day', -7, now())
GROUP BY 1`;

const SQL_TRANSLATED = `SELECT date_trunc('day', ts) AS d,
       count(*) AS n
FROM events
WHERE country = 'US'
  AND ts >= now() - INTERVAL 7 DAY
GROUP BY 1`;

type Phase =
  | 'ask'
  | 'guardrails'
  | 'translate'
  | 'route'
  | 'cache'
  | 'engine'
  | 'trail'
  | 'hold';

type PhaseMeta = {
  id: Phase;
  ms: number;
  label: string;
  caption: string;
  detail: string;
};

const PHASE_ORDER: Phase[] = [
  'ask',
  'guardrails',
  'translate',
  'route',
  'cache',
  'engine',
  'trail',
  'hold',
];

const PHASES: PhaseMeta[] = [
  {
    id: 'ask',
    ms: 2400,
    label: 'Ask',
    caption: 'The agent asks over MCP',
    detail: 'The AI agent calls queryflux.run_query — one endpoint, no warehouse credentials on the client.',
  },
  {
    id: 'guardrails',
    ms: 2200,
    label: 'Guardrails',
    caption: 'Guardrails check the query first',
    detail: 'Read-only, row limits, and policy checks run in QueryFlux — agents cannot talk their way around them.',
  },
  {
    id: 'translate',
    ms: 3200,
    label: 'Translate',
    caption: 'Dialect translation',
    detail:
      'sqlglot rewrites Trino-shaped SQL for DuckDB — date_add becomes interval arithmetic before dispatch.',
  },
  {
    id: 'route',
    ms: 2800,
    label: 'Route',
    caption: 'Route to the right engine',
    detail: 'Routers match protocol, tags, and SQL shape, then pick the cluster group that should run it.',
  },
  {
    id: 'cache',
    ms: 2000,
    label: 'Cache',
    caption: 'Result cache lookup',
    detail: 'Deterministic repeats can hit the result cache. This query misses — so it continues to the engine.',
  },
  {
    id: 'engine',
    ms: 2600,
    label: 'Engine',
    caption: 'Isolated DuckDB scratchpad',
    detail:
      'DuckDB gives the agent a dedicated analytical pad — query and verify locally without leaning on shared warehouses.',
  },
  {
    id: 'trail',
    ms: 2800,
    label: 'Trail',
    caption: 'Studio keeps the trail',
    detail: '12,480 rows in 48 ms — with route, guards, translation, and agent context ready to replay.',
  },
  {
    id: 'hold',
    ms: 2000,
    label: 'Trail',
    caption: 'Studio keeps the trail',
    detail: '12,480 rows in 48 ms — with route, guards, translation, and agent context ready to replay.',
  },
];

const STEP_BOXES = PHASES.filter((p) => p.id !== 'hold');

/** True once the animation has reached `id` (or any later phase). */
function phaseAtOrAfter(id: Phase, current: Phase): boolean {
  return PHASE_ORDER.indexOf(current) >= PHASE_ORDER.indexOf(id);
}

/** The AI agent, drawn as SVG so it stays sharp at any size and needs no backdrop. */
function AgentBot(): ReactNode {
  return (
    <svg className={styles.agentBot} viewBox="0 0 96 96" aria-hidden focusable="false">
      <defs>
        <linearGradient id="qfBotShell" x1="13" y1="44" x2="83" y2="66" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#9b4dff" />
          <stop offset="0.5" stopColor="#e5609e" />
          <stop offset="1" stopColor="#ff7a1a" />
        </linearGradient>
        <linearGradient id="qfBotShade" x1="48" y1="24" x2="48" y2="86" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#fff" stopOpacity="0.12" />
          <stop offset="0.35" stopColor="#fff" stopOpacity="0" />
          <stop offset="1" stopColor="#1a0630" stopOpacity="0.45" />
        </linearGradient>
        <linearGradient id="qfBotVisor" x1="48" y1="40" x2="48" y2="72" gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#1d1533" />
          <stop offset="1" stopColor="#0a0714" />
        </linearGradient>
        <radialGradient id="qfBotBulb" cx="0.38" cy="0.35" r="0.7">
          <stop offset="0" stopColor="#ffd2a8" />
          <stop offset="0.5" stopColor="#ff8a2a" />
          <stop offset="1" stopColor="#e2580b" />
        </radialGradient>
        <filter id="qfBotGlow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="1.6" result="b" />
          <feMerge>
            <feMergeNode in="b" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>
      <path d="M48 26 V15" stroke="#160b26" strokeWidth="5.5" strokeLinecap="round" />
      <path d="M48 26 V15" stroke="#8b5cf6" strokeWidth="3" strokeLinecap="round" />
      <circle className={styles.agentBotBulb} cx="48" cy="12" r="5" fill="url(#qfBotBulb)" stroke="#160b26" strokeWidth="2.5" />
      <circle cx="46.4" cy="10.4" r="1.4" fill="#fff" opacity="0.8" />
      <rect x="8" y="46" width="8" height="18" rx="4" fill="#8b3df5" stroke="#160b26" strokeWidth="2.5" />
      <rect x="80" y="46" width="8" height="18" rx="4" fill="#ff7a1a" stroke="#160b26" strokeWidth="2.5" />
      <rect x="13" y="25" width="70" height="60" rx="27" fill="url(#qfBotShell)" />
      <rect x="13" y="25" width="70" height="60" rx="27" fill="url(#qfBotShade)" />
      <rect x="13" y="25" width="70" height="60" rx="27" fill="none" stroke="#160b26" strokeWidth="2.5" />
      <ellipse cx="30" cy="34.5" rx="8" ry="3.6" transform="rotate(-26 30 34.5)" fill="#fff" opacity="0.5" />
      <circle cx="22.5" cy="42" r="1.6" fill="#fff" opacity="0.45" />
      <rect x="21" y="39" width="54" height="33" rx="16.5" fill="url(#qfBotVisor)" />
      <rect x="21.75" y="39.75" width="52.5" height="31.5" rx="15.75" fill="none" stroke="#fff" strokeOpacity="0.08" strokeWidth="1.5" />
      <g className={styles.agentBotEyes}>
        <g filter="url(#qfBotGlow)" fill="#a3e635">
          <ellipse cx="38" cy="54" rx="5" ry="6" />
          <ellipse cx="58" cy="54" rx="5" ry="6" />
        </g>
        <circle cx="36.3" cy="51.2" r="1.9" fill="#fff" opacity="0.9" />
        <circle cx="56.3" cy="51.2" r="1.9" fill="#fff" opacity="0.9" />
      </g>
      <path
        d="M42.5 62.5 Q48 67 53.5 62.5"
        fill="none"
        stroke="#a3e635"
        strokeWidth="2.6"
        strokeLinecap="round"
        filter="url(#qfBotGlow)"
      />
    </svg>
  );
}

/** Tracks the visitor's reduced-motion preference, updating if it changes. */
function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia('(prefers-reduced-motion: reduce)');
    setReduced(mq.matches);
    const onChange = () => setReduced(mq.matches);
    mq.addEventListener('change', onChange);
    return () => mq.removeEventListener('change', onChange);
  }, []);
  return reduced;
}

/** Wraps the first occurrence of `needle` in `sql` in a highlight mark. */
function highlightDiff(sql: string, needle: string, kind: 'from' | 'to'): ReactNode {
  const i = sql.indexOf(needle);
  if (i < 0) {
    return sql;
  }
  return (
    <>
      {sql.slice(0, i)}
      <mark className={kind === 'from' ? styles.sqlDiffFrom : styles.sqlDiffTo}>{needle}</mark>
      {sql.slice(i + needle.length)}
    </>
  );
}

/** Looping diagram of one agent query: guardrails, translation, routing, the engine and back. */
export default function HomeAgentFlow(): ReactNode {
  const reduced = usePrefersReducedMotion();
  const [phaseIndex, setPhaseIndex] = useState(reduced ? PHASES.length - 2 : 0);
  const [engineTick, setEngineTick] = useState(0);
  const phase = PHASES[phaseIndex];
  const pickIndex = ENGINE_DEFS.findIndex((e) => e.id === PICK);

  const duckLogo = useBaseUrl('/img/engines/duckdb.png');
  const trinoLogo = useBaseUrl('/img/engines/trino.png');
  const starrocksLogo = useBaseUrl('/img/engines/starrocks.svg');
  const queryfluxCrab = useBaseUrl('/img/queryflux-crab.png');
  const engines: Engine[] = [
    {...ENGINE_DEFS[0], logo: duckLogo},
    {...ENGINE_DEFS[1], logo: trinoLogo},
    {...ENGINE_DEFS[2], logo: starrocksLogo},
  ];

  useEffect(() => {
    if (reduced) {
      setPhaseIndex(PHASES.length - 2);
      return;
    }
    const t = window.setTimeout(() => {
      setPhaseIndex((i) => (i + 1) % PHASES.length);
    }, phase.ms);
    return () => window.clearTimeout(t);
  }, [phaseIndex, phase.ms, reduced]);

  useEffect(() => {
    if (reduced || phase.id !== 'route') {
      return;
    }
    setEngineTick(0);
    let n = 0;
    const id = window.setInterval(() => {
      n += 1;
      setEngineTick(n);
    }, 240);
    return () => window.clearInterval(id);
  }, [phase.id, reduced]);

  const fanPathRefs = useRef<(SVGPathElement | null)[]>([]);
  const fanPacketRef = useRef<SVGGElement | null>(null);

  /**
   * Engine phase: SQL travels from QueryFlux along the chosen path to the engine, waits
   * while it runs, then the rows travel back. Drawn with getPointAtLength so the packet
   * follows the real curve at whatever size the SVG is rendered.
   */
  useEffect(() => {
    const g = fanPacketRef.current;
    const path = fanPathRefs.current[pickIndex];
    if (!g || !path) {
      return;
    }
    if (reduced || phase.id !== 'engine') {
      g.style.display = 'none';
      return;
    }
    const text = g.querySelector('text');
    const total = path.getTotalLength();
    const duration = phase.ms * 0.92;
    const ease = (x: number) => (x < 0.5 ? 2 * x * x : 1 - (-2 * x + 2) ** 2 / 2);
    const OUT = 0.4; // share of the phase spent travelling to the engine
    const HOLD = 0.18; // share spent waiting at the engine
    let raf = 0;
    const start = performance.now();
    const frame = (now: number) => {
      const p = Math.min((now - start) / duration, 1);
      let along: number;
      let kind: 'sql' | 'rows';
      if (p < OUT) {
        along = ease(p / OUT);
        kind = 'sql';
      } else if (p < OUT + HOLD) {
        along = 1;
        kind = 'sql';
      } else {
        along = 1 - ease((p - OUT - HOLD) / (1 - OUT - HOLD));
        kind = 'rows';
      }
      const pt = path.getPointAtLength(along * total);
      g.setAttribute('transform', `translate(${pt.x} ${pt.y})`);
      g.setAttribute('data-kind', kind);
      if (text) {
        text.textContent = kind === 'sql' ? 'SQL' : 'ROWS';
      }
      g.style.display = '';
      if (p < 1) {
        raf = requestAnimationFrame(frame);
      }
    };
    raf = requestAnimationFrame(frame);
    return () => {
      cancelAnimationFrame(raf);
      g.style.display = 'none';
    };
  }, [phase.id, phase.ms, pickIndex, reduced]);

  const routingSettled =
    reduced ||
    phaseAtOrAfter('cache', phase.id) ||
    (phase.id === 'route' && engineTick >= 7);

  const activeEngine =
    phase.id === 'route' && !routingSettled
      ? engineTick % engines.length
      : pickIndex;

  const showAnswer = phase.id === 'trail' || phase.id === 'hold' || reduced;
  const showTranslate = phase.id === 'translate';
  const linesActive = phaseAtOrAfter('route', phase.id);

  /** Continuous motion: inbound, processing, outbound, return. */
  const wireMode =
    phase.id === 'ask'
      ? 'in'
      : phase.id === 'engine'
        ? 'out'
        : phase.id === 'trail'
          ? 'back'
          : phase.id === 'hold'
            ? 'idle'
            : 'process';

  const showEngineFlow = phase.id === 'engine';

  const proxyLit = phase.id !== 'ask' || reduced;
  const agentLit =
    phase.id === 'ask' || phase.id === 'trail' || phase.id === 'hold' || reduced;

  const sqlLabel =
    phase.id === 'ask'
      ? 'Incoming · Trino SQL'
      : phase.id === 'guardrails'
        ? 'Guarded'
        : phase.id === 'route'
          ? 'Routed'
          : phase.id === 'cache'
            ? 'Cache miss'
            : phase.id === 'engine'
              ? 'Executing · DuckDB'
              : 'SQL';

  const sqlText = phaseAtOrAfter('translate', phase.id) ? SQL_TRANSLATED : SQL_SOURCE;

  const tagFocus = {
    guardrails: phase.id === 'guardrails',
    translate: phase.id === 'translate',
    route: phase.id === 'route',
    cache: phase.id === 'cache',
  };
  const tagOn = {
    guardrails: phaseAtOrAfter('guardrails', phase.id),
    translate: phaseAtOrAfter('translate', phase.id),
    route: phaseAtOrAfter('route', phase.id),
    cache: phaseAtOrAfter('cache', phase.id),
  };

  const statusChips = [
    {key: 'g', text: 'read-only · ok', on: phaseAtOrAfter('guardrails', phase.id)},
    {key: 't', text: 'trino → duckdb', on: phaseAtOrAfter('translate', phase.id)},
    {
      key: 'r',
      text: '→ duckdb-adhoc',
      on: phaseAtOrAfter('route', phase.id) && routingSettled,
    },
    {key: 'c', text: 'cache miss', on: phaseAtOrAfter('cache', phase.id)},
    {key: 'e', text: '48 ms', on: phaseAtOrAfter('engine', phase.id)},
  ];

  return (
    <div
      className={styles.flow}
      data-phase={phase.id}
      aria-label="Live diagram of a query flowing through QueryFlux features to an engine">
      <div className={styles.diagram}>
        <div className={styles.diagramQuestionBar}>
          <span>You asked</span>
          <p>{QUESTION}</p>
        </div>

        <div className={styles.diagramTop}>
          <div
            className={clsx(
              styles.diagramNode,
              styles.diagramAgent,
              agentLit && styles.diagramNodeLit,
            )}>
            <span className={styles.diagramAgentRing} aria-hidden>
              <span className={styles.diagramAgentAvatar}>
                <AgentBot />
              </span>
              <span className={styles.diagramAgentStatus} />
            </span>
            <strong>AI agent</strong>
            <span className={styles.diagramNodeChip}>MCP client</span>
          </div>

          <div
            className={clsx(
              styles.diagramWire,
              wireMode !== 'idle' && styles.diagramWireHot,
              wireMode === 'process' && styles.diagramWireProcess,
            )}
            data-mode={wireMode}
            aria-hidden>
            <span className={styles.diagramWireLine} />
            {wireMode === 'in' || wireMode === 'back' ? (
              <span
                key={`wire-${phase.id}`}
                className={clsx(
                  styles.diagramPacket,
                  wireMode === 'back' && styles.diagramPacketBack,
                )}>
                {wireMode === 'back' ? 'rows' : 'SQL'}
              </span>
            ) : null}
            {wireMode === 'process' ? (
              <span className={styles.diagramWirePulse} />
            ) : null}
          </div>

          <div
            className={clsx(
              styles.diagramNode,
              styles.diagramProxy,
              proxyLit && styles.diagramNodeLit,
              (phase.id === 'guardrails' ||
                phase.id === 'translate' ||
                phase.id === 'route' ||
                phase.id === 'cache') &&
                styles.diagramNodePulse,
            )}>
            <div className={styles.diagramProxyHead}>
              <span className={styles.diagramProxyStage} aria-hidden>
                <img
                  className={styles.diagramProxyMascot}
                  src={queryfluxCrab}
                  alt=""
                  width={176}
                  height={76}
                />
              </span>
              <div className={styles.diagramProxyTitles}>
                <span className={styles.diagramNodeEyebrow}>Control plane</span>
                <strong>QueryFlux</strong>
              </div>
            </div>
            <div className={styles.diagramProxyTags}>
              {(
                [
                  ['guardrails', tagOn.guardrails, tagFocus.guardrails],
                  ['translate', tagOn.translate, tagFocus.translate],
                  ['route', tagOn.route, tagFocus.route],
                  ['cache', tagOn.cache, tagFocus.cache],
                ] as const
              ).map(([name, on, focus]) => (
                <span
                  key={name}
                  className={clsx(
                    styles.diagramTag,
                    on && styles.diagramTagOn,
                    focus && styles.diagramTagFocus,
                  )}>
                  {name}
                </span>
              ))}
            </div>
          </div>

          <div
            className={styles.diagramFan}
            data-active={linesActive ? 'true' : 'false'}>
            <svg
              className={styles.diagramFanSvg}
              viewBox="0 0 100 180"
              preserveAspectRatio="xMidYMid meet"
              aria-hidden>
              {engines.map((engine, i) => {
                const isPick = engine.id === PICK;
                const scanning = phase.id === 'route' && !routingSettled && i === activeEngine;
                const chosen = routingSettled && isPick;
                const dim = routingSettled && !isPick;
                const flowing = showEngineFlow && isPick;
                return (
                  <path
                    key={engine.id}
                    ref={(el) => {
                      fanPathRefs.current[i] = el;
                    }}
                    d={FAN_PATHS[i]}
                    className={clsx(
                      styles.diagramFanPath,
                      scanning && styles.diagramFanPathScan,
                      chosen && styles.diagramFanPathHot,
                      dim && styles.diagramFanPathDim,
                      flowing && styles.diagramFanPathFlow,
                    )}
                    fill="none"
                  />
                );
              })}
              <g ref={fanPacketRef} className={styles.fanPacket} style={{display: 'none'}}>
                <rect x={-17} y={-7.5} width={34} height={15} rx={7.5} />
                <text x={0} y={0.5} textAnchor="middle" dominantBaseline="middle">
                  SQL
                </text>
              </g>
            </svg>

            <ul className={styles.diagramEngineList}>
              {engines.map((engine, i) => {
                const selected =
                  (routingSettled && engine.id === PICK) ||
                  (!routingSettled && phase.id === 'route' && i === activeEngine);
                const dimmed = routingSettled && engine.id !== PICK;
                const running = phase.id === 'engine' && engine.id === PICK;
                const arrived = phaseAtOrAfter('engine', phase.id) && engine.id === PICK;
                return (
                  <li
                    key={engine.id}
                    className={clsx(
                      styles.diagramEngine,
                      selected && styles.diagramEngineSelected,
                      dimmed && styles.diagramEngineDim,
                      running && styles.diagramEngineRunning,
                      arrived && styles.diagramEngineArrived,
                    )}>
                    <img src={engine.logo} alt="" width={32} height={32} />
                    <span>
                      <strong>{engine.label}</strong>
                      <em>{engine.hint}</em>
                    </span>
                  </li>
                );
              })}
            </ul>
          </div>
        </div>

        <div
          className={styles.diagramSql}
          data-tone={showAnswer ? 'answer' : showTranslate ? 'translate' : phase.id}>
          <div
            className={clsx(
              styles.diagramSqlPane,
              !showAnswer && !showTranslate && styles.diagramSqlPaneActive,
            )}
            aria-hidden={showAnswer || showTranslate}>
            <p className={styles.diagramSqlText}>
              <span className={styles.diagramSqlLabel}>{sqlLabel}</span>
              <code>{sqlText}</code>
            </p>
          </div>

          <div
            className={clsx(
              styles.diagramSqlPane,
              showTranslate && styles.diagramSqlPaneActive,
            )}
            aria-hidden={!showTranslate}>
            <div className={styles.sqlTranslate}>
              <div className={styles.sqlTranslateCol}>
                <span className={styles.sqlTranslateHead}>
                  <span className={styles.sqlDiffFromBadge}>From</span> Trino
                </span>
                <pre>
                  <code>
                    {highlightDiff(SQL_SOURCE, "date_add('day', -7, now())", 'from')}
                  </code>
                </pre>
              </div>
              <div className={styles.sqlTranslateArrow} aria-hidden>
                →
              </div>
              <div className={styles.sqlTranslateCol}>
                <span className={styles.sqlTranslateHead}>
                  <span className={styles.sqlDiffToBadge}>To</span> DuckDB
                </span>
                <pre>
                  <code>
                    {highlightDiff(SQL_TRANSLATED, 'now() - INTERVAL 7 DAY', 'to')}
                  </code>
                </pre>
              </div>
            </div>
          </div>

          <div
            className={clsx(styles.diagramSqlPane, showAnswer && styles.diagramSqlPaneActive)}
            aria-hidden={!showAnswer}>
            <div className={styles.diagramAnswerBlock}>
              <p className={styles.diagramAnswer}>
                There were <strong>12,480</strong> US events in the last 7 days.
              </p>
              <dl className={styles.diagramResultMeta}>
                <div>
                  <dt>Engine</dt>
                  <dd>DuckDB</dd>
                </div>
                <div>
                  <dt>Duration</dt>
                  <dd>48 ms</dd>
                </div>
                <div>
                  <dt>Route</dt>
                  <dd>duckdb-adhoc</dd>
                </div>
                <div>
                  <dt>Studio</dt>
                  <dd>replay ready</dd>
                </div>
              </dl>
            </div>
          </div>
        </div>

        <ul className={styles.diagramStatus} aria-hidden>
          {statusChips.map((chip) => (
            <li key={chip.key} className={clsx(chip.on && styles.diagramStatusOn)}>
              {chip.text}
            </li>
          ))}
        </ul>
      </div>

      <div className={styles.diagramCaption} aria-live="polite">
        <ol className={styles.diagramSteps} aria-label="QueryFlux feature steps">
          {STEP_BOXES.map((p, i) => {
            const active =
              phase.id === p.id || (phase.id === 'hold' && p.id === 'trail');
            const done = !active && (phase.id === 'hold' || i < phaseIndex);
            return (
              <li key={p.id}>
                <button
                  type="button"
                  className={clsx(
                    styles.diagramStepBox,
                    active && styles.diagramStepBoxActive,
                    done && styles.diagramStepBoxDone,
                  )}
                  aria-current={active ? 'step' : undefined}
                  onClick={() => setPhaseIndex(i)}>
                  <span className={styles.diagramStepNum}>
                    {String(i + 1).padStart(2, '0')}
                  </span>
                  <span className={styles.diagramStepLabel}>{p.label}</span>
                </button>
              </li>
            );
          })}
        </ol>
        <div key={phase.id} className={styles.diagramCaptionCopy}>
          <p className={styles.diagramCaptionTitle}>{phase.caption}</p>
          <p className={styles.diagramCaptionBody}>{phase.detail}</p>
        </div>
      </div>
    </div>
  );
}
