import {useEffect, useState, type ReactNode} from 'react';
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

function phaseAtOrAfter(id: Phase, current: Phase): boolean {
  return PHASE_ORDER.indexOf(current) >= PHASE_ORDER.indexOf(id);
}

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

export default function HomeAgentFlow(): ReactNode {
  const reduced = usePrefersReducedMotion();
  const [phaseIndex, setPhaseIndex] = useState(reduced ? PHASES.length - 2 : 0);
  const [engineTick, setEngineTick] = useState(0);
  const phase = PHASES[phaseIndex];
  const pickIndex = ENGINE_DEFS.findIndex((e) => e.id === PICK);

  const duckLogo = useBaseUrl('/img/engines/duckdb.png');
  const trinoLogo = useBaseUrl('/img/engines/trino.png');
  const starrocksLogo = useBaseUrl('/img/engines/starrocks.svg');
  const agentBot = useBaseUrl('/img/ai-agent-avatar.jpg');
  const queryfluxCrab = useBaseUrl('/img/queryflux-mascot.png');
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
            <img
              className={styles.diagramAgentAvatar}
              src={agentBot}
              alt=""
              width={48}
              height={48}
            />
            <strong>AI agent</strong>
            <span className={styles.diagramNodeHint}>asks over MCP</span>
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
              <img
                className={styles.diagramProxyMascot}
                src={queryfluxCrab}
                alt=""
                width={52}
                height={36}
              />
              <div className={styles.diagramProxyTitles}>
                <span className={styles.diagramNodeEyebrow}>Control plane</span>
                <strong>QueryFlux</strong>
                <span className={styles.diagramNodeHint}>guard · translate · route · cache</span>
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
