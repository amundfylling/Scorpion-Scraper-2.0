import React from 'react';
import {AbsoluteFill, interpolate, useCurrentFrame} from 'remotion';
import {
	Arrow,
	Browser,
	C,
	Captions,
	Card,
	Heading,
	Hl,
	Robot,
	Scene,
	Sheet,
	usePop,
	useProgress,
} from './ui';

// Example names are made up; the real scraper reads whatever the site lists.

const S = 30; // frames per second

export const DURATIONS = {
	intro: 6 * S,
	problem: 11 * S,
	nightly: 12 * S,
	tournaments: 17 * S,
	matches: 17 * S,
	rules: 17 * S,
	players: 14 * S,
	polite: 13 * S,
	saved: 12 * S,
	recap: 12 * S,
};

/* ------------------------------------------------------------------ */

export const Intro: React.FC = () => {
	const d = DURATIONS.intro;
	const title = usePop(5);
	const sub = usePop(25);
	const robot = usePop(40);
	return (
		<Scene duration={d}>
			<AbsoluteFill style={{alignItems: 'center', justifyContent: 'center'}}>
				<div style={{transform: `scale(${robot})`, marginBottom: 30}}>
					<Robot size={190} />
				</div>
				<div
					style={{
						fontSize: 96,
						fontWeight: 900,
						opacity: title,
						transform: `translateY(${(1 - title) * 40}px)`,
						textAlign: 'center',
					}}
				>
					How <Hl>Scorpion Scraper</Hl> gets its data
				</div>
				<div
					style={{
						fontSize: 44,
						color: C.muted,
						marginTop: 24,
						opacity: sub,
					}}
				>
					A step-by-step tour, no tech knowledge needed
				</div>
			</AbsoluteFill>
		</Scene>
	);
};

/* ------------------------------------------------------------------ */

export const Problem: React.FC = () => {
	const d = DURATIONS.problem;
	const site = usePop(10);
	const robot = usePop(130);
	const arrow = useProgress(150, 190);
	const sheet = usePop(185);
	// The stack of pages fans out to show "thousands of pages".
	const fan = useProgress(30, 90);
	return (
		<Scene duration={d}>
			<Heading title="Where the data comes from" />
			<div
				style={{
					position: 'absolute',
					left: 140,
					top: 260,
					opacity: site,
					transform: `scale(${0.8 + 0.2 * site})`,
				}}
			>
				{[4, 3, 2, 1, 0].map((i) => (
					<div
						key={i}
						style={{
							position: 'absolute',
							left: i * 26 * fan,
							top: i * 22 * fan,
							opacity: i === 0 ? 1 : 0.9 - i * 0.12,
						}}
					>
						<Browser url="th.sportscorpion.com" width={560} height={390}>
							{i === 0 ? (
								<div>
									<div style={{fontSize: 30, fontWeight: 800, marginBottom: 16}}>
										Table hockey results
									</div>
									{['Oslo Open', 'Riga Cup', 'Prague Masters', 'Helsinki Trophy', 'Tallinn Spring'].map(
										(t, k) => (
											<div
												key={t}
												style={{
													fontSize: 24,
													padding: '8px 0',
													borderBottom: '1px solid #ddd',
													color: k % 2 ? '#3a4a60' : '#1d2633',
												}}
											>
												🏆 {t}
											</div>
										),
									)}
								</div>
							) : null}
						</Browser>
					</div>
				))}
			</div>
			<div
				style={{
					position: 'absolute',
					left: 860,
					top: 360,
					transform: `scale(${robot})`,
				}}
			>
				<Robot size={150} />
			</div>
			<Arrow x1={1030} y1={470} x2={1170} y2={470} progress={arrow} />
			<div
				style={{
					position: 'absolute',
					left: 1190,
					top: 300,
					opacity: sheet,
					transform: `translateY(${(1 - sheet) * 30}px)`,
				}}
			>
				<Sheet
					title="Neat tables"
					width={600}
					columns={['Player 1', 'Score', 'Player 2']}
					rows={[
						['Anna Berg', '5 – 3', 'Ole Hansen'],
						['Mia Lind', '2 – 4', 'Jon Dahl'],
						['Ole Hansen', '6 – 1', 'Mia Lind'],
					]}
				/>
			</div>
			<Captions
				lines={[
					{
						from: 15,
						text: (
							<>
								All results live on the <Hl>SportScorpion</Hl> website, spread over <Hl>thousands</Hl> of
								pages.
							</>
						),
					},
					{
						from: 125,
						text: (
							<>
								The scraper is a little <Hl color={C.teal}>robot</Hl> that visits those pages, the way you
								would in a browser…
							</>
						),
					},
					{
						from: 210,
						text: <>…and copies everything it reads into neat tables that are easy to analyse.</>,
					},
				]}
			/>
		</Scene>
	);
};

/* ------------------------------------------------------------------ */

const StepCard: React.FC<{n: number; icon: string; title: string; text: string; delay: number}> = ({
	n,
	icon,
	title,
	text,
	delay,
}) => {
	const p = usePop(delay);
	return (
		<Card
			style={{
				width: 440,
				padding: 36,
				opacity: p,
				transform: `translateY(${(1 - p) * 60}px)`,
			}}
		>
			<div style={{display: 'flex', alignItems: 'center', gap: 18}}>
				<div
					style={{
						width: 64,
						height: 64,
						borderRadius: 32,
						background: C.orange,
						color: C.bg,
						fontSize: 36,
						fontWeight: 900,
						display: 'flex',
						alignItems: 'center',
						justifyContent: 'center',
					}}
				>
					{n}
				</div>
				<div style={{fontSize: 54}}>{icon}</div>
			</div>
			<div style={{fontSize: 44, fontWeight: 800, marginTop: 22}}>{title}</div>
			<div style={{fontSize: 28, color: C.muted, marginTop: 10, lineHeight: 1.35}}>{text}</div>
		</Card>
	);
};

const Clock: React.FC<{progress: number}> = ({progress}) => {
	// Hands sweep up to midnight.
	const minute = -90 + 360 * progress * 2;
	const hour = -30 + 30 * progress;
	return (
		<svg width="220" height="220" viewBox="0 0 220 220">
			<circle cx="110" cy="110" r="100" fill={C.card} stroke={C.teal} strokeWidth="8" />
			{Array.from({length: 12}).map((_, i) => {
				const a = (i / 12) * Math.PI * 2;
				return (
					<circle
						key={i}
						cx={110 + Math.sin(a) * 80}
						cy={110 - Math.cos(a) * 80}
						r={i % 3 === 0 ? 7 : 4}
						fill={C.muted}
					/>
				);
			})}
			<line
				x1="110"
				y1="110"
				x2="110"
				y2="55"
				stroke={C.text}
				strokeWidth="9"
				strokeLinecap="round"
				transform={`rotate(${hour} 110 110)`}
			/>
			<line
				x1="110"
				y1="110"
				x2="110"
				y2="35"
				stroke={C.orange}
				strokeWidth="6"
				strokeLinecap="round"
				transform={`rotate(${minute + 90} 110 110)`}
			/>
			<circle cx="110" cy="110" r="9" fill={C.orange} />
		</svg>
	);
};

export const Nightly: React.FC = () => {
	const d = DURATIONS.nightly;
	const frame = useCurrentFrame();
	const clockP = useProgress(10, 70);
	const ring = frame > 70 && frame < 110 ? Math.sin(frame * 1.6) * 8 : 0;
	const arrows = [useProgress(170, 190), useProgress(200, 220)];
	return (
		<Scene duration={d}>
			<Heading title="Every night, all by itself" />
			<div
				style={{
					position: 'absolute',
					left: 1500,
					top: 50,
					transform: `rotate(${ring}deg) scale(0.85)`,
				}}
			>
				<Clock progress={clockP} />
				<div style={{textAlign: 'center', fontSize: 40, fontWeight: 800, color: C.teal}}>00:00</div>
			</div>
			<div
				style={{
					position: 'absolute',
					top: 330,
					left: 0,
					right: 0,
					display: 'flex',
					justifyContent: 'center',
					gap: 110,
				}}
			>
				<StepCard
					n={1}
					icon="🏆"
					title="Tournaments"
					text="Find tournaments that are new on the website."
					delay={120}
				/>
				<StepCard n={2} icon="🏒" title="Matches" text="Read every game played in them." delay={150} />
				<StepCard n={3} icon="👤" title="Players" text="Look up anyone it hasn't met before." delay={180} />
			</div>
			<Arrow x1={645} y1={490} x2={730} y2={490} progress={arrows[0]} />
			<Arrow x1={1195} y1={490} x2={1280} y2={490} progress={arrows[1]} />
			<Captions
				lines={[
					{
						from: 15,
						text: (
							<>
								Every night at <Hl>midnight</Hl>, GitHub (where the project lives) wakes the robot up
								automatically.
							</>
						),
					},
					{
						from: 120,
						text: (
							<>
								It always does three jobs <Hl>in this order</Hl>, because each job uses what the one before
								it found.
							</>
						),
					},
				]}
			/>
		</Scene>
	);
};

/* ------------------------------------------------------------------ */

const archiveRows = [
	{name: 'Oslo Open 2026', isNew: true},
	{name: 'Riga Cup', isNew: true},
	{name: 'Prague Masters', isNew: false},
	{name: 'Helsinki Trophy', isNew: false},
	{name: 'Tallinn Spring', isNew: false},
	{name: 'Stockholm League #4', isNew: false},
];

export const Tournaments: React.FC = () => {
	const d = DURATIONS.tournaments;
	const frame = useCurrentFrame();
	const browser = usePop(10);
	// Scanning beam moves down the list.
	const scan = useProgress(60, 170);
	const scanRow = Math.floor(scan * archiveRows.length - 0.001);
	const pageCounter = Math.min(5, 1 + Math.floor(useProgress(60, 200) * 5));
	const detail = usePop(260);
	const detailRows = useProgress(280, 380);
	const arrow = useProgress(390, 430);
	const sheet = usePop(420);
	return (
		<Scene duration={d}>
			<Heading step="Step 1" title="Find new tournaments" />
			<div
				style={{
					position: 'absolute',
					left: 110,
					top: 250,
					opacity: browser,
					transform: `scale(${0.85 + 0.15 * browser})`,
					transformOrigin: 'top left',
				}}
			>
				<Browser url="sportscorpion.com › tournament archive" width={680} height={560}>
					<div style={{display: 'flex', justifyContent: 'space-between', marginBottom: 12}}>
						<div style={{fontSize: 30, fontWeight: 800}}>Tournament archive</div>
						<div
							style={{
								fontSize: 24,
								fontWeight: 700,
								background: '#1d2633',
								color: 'white',
								borderRadius: 10,
								padding: '4px 14px',
							}}
						>
							page {pageCounter} of 5
						</div>
					</div>
					<div style={{fontSize: 20, color: '#6b778a', marginBottom: 8}}>Newest first ↓</div>
					{archiveRows.map((row, i) => {
						const seen = frame >= 60 && i <= scanRow;
						return (
							<div
								key={row.name}
								style={{
									display: 'flex',
									justifyContent: 'space-between',
									alignItems: 'center',
									fontSize: 26,
									padding: '12px 10px',
									borderBottom: '1px solid #ddd',
									background: i === scanRow && scan < 1 ? '#ffe7d3' : 'transparent',
									borderRadius: 8,
								}}
							>
								<span>🏆 {row.name}</span>
								{seen ? (
									<span
										style={{
											fontSize: 20,
											fontWeight: 800,
											padding: '4px 12px',
											borderRadius: 8,
											background: row.isNew ? C.green : '#d5dbe4',
											color: row.isNew ? '#0b3d1f' : '#4b5668',
										}}
									>
										{row.isNew ? 'NEW → open it' : 'already saved, skip'}
									</span>
								) : null}
							</div>
						);
					})}
				</Browser>
			</div>

			<Card
				style={{
					position: 'absolute',
					left: 840,
					top: 250,
					width: 400,
					padding: 30,
					opacity: detail,
					transform: `translateX(${(1 - detail) * 80}px)`,
				}}
			>
				<div style={{fontSize: 22, color: C.muted, fontWeight: 700, letterSpacing: 2}}>
					OPENS THE TOURNAMENT PAGE
				</div>
				<div style={{fontSize: 36, fontWeight: 800, margin: '10px 0 18px'}}>🏆 Oslo Open 2026</div>
				{[
					['Date', '24 Oct 2026'],
					['Type', 'Individual'],
					['Status', 'Finished'],
					['Stages', 'Group A, Group B, Playoff'],
				].map(([k, v], i) => (
					<div
						key={k}
						style={{
							display: 'flex',
							justifyContent: 'space-between',
							fontSize: 26,
							padding: '10px 0',
							borderTop: `1px solid ${C.line}`,
							opacity: detailRows * 4 > i ? 1 : 0,
						}}
					>
						<span style={{color: C.muted}}>{k}</span>
						<span style={{fontWeight: 700, textAlign: 'right'}}>{v}</span>
					</div>
				))}
			</Card>
			<Arrow x1={1255} y1={430} x2={1335} y2={430} progress={arrow} />
			<div
				style={{
					position: 'absolute',
					left: 1350,
					top: 250,
					opacity: sheet,
					transform: `translateY(${(1 - sheet) * 30}px)`,
				}}
			>
				<Sheet
					title="Tournament list"
					width={460}
					columns={['Tournament', 'Type']}
					rows={[
						['Prague Masters', 'Individual'],
						['Helsinki Trophy', 'Team'],
						['Riga Cup', 'Individual'],
						['Oslo Open 2026', 'Individual'],
					]}
					highlightLast
				/>
			</div>
			<Captions
				lines={[
					{
						from: 15,
						text: (
							<>
								The robot opens the website's <Hl>tournament archive</Hl>. Newest tournaments are listed
								first.
							</>
						),
					},
					{
						from: 100,
						text: (
							<>
								It only flips through the first <Hl>5 pages</Hl>. It runs every night, so anything new is
								always near the top.
							</>
						),
					},
					{
						from: 210,
						text: (
							<>
								Tournaments it already knows are <Hl color={C.muted}>skipped</Hl>. Only the{' '}
								<Hl color={C.green}>new</Hl> ones are opened.
							</>
						),
					},
					{
						from: 290,
						text: (
							<>
								From each one it notes the date, type, whether it's finished, and its <Hl>stages</Hl>{' '}
								(groups, playoffs…).
							</>
						),
					},
					{
						from: 410,
						text: <>Then it adds the tournament to its list.</>,
					},
				]}
			/>
		</Scene>
	);
};

/* ------------------------------------------------------------------ */

const games = [
	['Anna Berg', '5', '3', 'Ole Hansen', ''],
	['Mia Lind', '2', '4', 'Jon Dahl', ''],
	['Ole Hansen', '3', '2', 'Mia Lind', 'OT'],
	['Jon Dahl', '1', '6', 'Anna Berg', ''],
];

export const Matches: React.FC = () => {
	const d = DURATIONS.matches;
	const frame = useCurrentFrame();
	const stage = usePop(10);
	const read = useProgress(110, 290);
	const readRow = Math.floor(read * games.length - 0.001);
	const sheetRows = frame < 110 ? 0 : Math.min(games.length, readRow + 1);
	const team = usePop(330);
	return (
		<Scene duration={d}>
			<Heading step="Step 2" title="Read every match" />
			<div
				style={{
					position: 'absolute',
					left: 110,
					top: 250,
					opacity: stage,
					transform: `scale(${0.85 + 0.15 * stage})`,
					transformOrigin: 'top left',
				}}
			>
				<Browser url="Oslo Open 2026 › Group A › matches" width={700} height={430}>
					<div style={{fontSize: 30, fontWeight: 800, marginBottom: 14}}>Group A · Round 1</div>
					{games.map(([p1, g1, g2, p2, ot], i) => (
						<div
							key={i}
							style={{
								display: 'grid',
								gridTemplateColumns: '1fr 130px 1fr',
								alignItems: 'center',
								fontSize: 26,
								padding: '14px 10px',
								borderBottom: '1px solid #ddd',
								borderRadius: 8,
								background: i === readRow && read < 1 ? '#ffe7d3' : 'transparent',
							}}
						>
							<span>{p1}</span>
							<span style={{textAlign: 'center', fontWeight: 800}}>
								{g1} : {g2}
								{ot ? <span style={{fontSize: 18, color: C.orange}}> OT</span> : null}
							</span>
							<span style={{textAlign: 'right'}}>{p2}</span>
						</div>
					))}
				</Browser>
			</div>
			<Arrow x1={840} y1={500} x2={930} y2={500} progress={useProgress(100, 130)} />
			<div style={{position: 'absolute', left: 960, top: 250}}>
				<Sheet
					title="Match list"
					width={850}
					columns={['Player 1', 'Goals', 'Goals', 'Player 2', 'Extra time?']}
					rows={games.map(([p1, g1, g2, p2, ot]) => [p1, g1, g2, p2, ot ? 'Yes' : 'No'])}
					visibleRows={sheetRows}
					highlightLast={read < 1}
				/>
				<div style={{fontSize: 22, color: C.muted, marginTop: 14}}>
					+ the tournament, stage, round and date for every row
				</div>
			</div>
			<Card
				style={{
					position: 'absolute',
					left: 960,
					top: 640,
					width: 850,
					padding: '22px 30px',
					opacity: team,
					transform: `translateY(${(1 - team) * 40}px)`,
					display: 'flex',
					alignItems: 'center',
					gap: 22,
				}}
			>
				<div style={{fontSize: 54}}>👥</div>
				<div style={{fontSize: 28, lineHeight: 1.35}}>
					<b>Team tournaments:</b> the robot opens each team match and saves the individual games inside it,
					not just the team score.
				</div>
			</Card>
			<Captions
				lines={[
					{
						from: 15,
						text: (
							<>
								For each tournament, the robot opens every <Hl>stage</Hl>: each group, round and playoff.
							</>
						),
					},
					{
						from: 110,
						text: (
							<>
								It reads the games <Hl>line by line</Hl>: who played, the score, and whether it went to extra
								time.
							</>
						),
					},
					{
						from: 330,
						text: <>Games that haven't been played yet are left out. Only real results are saved.</>,
					},
				]}
			/>
		</Scene>
	);
};

/* ------------------------------------------------------------------ */

const rules = [
	{
		icon: '🆕',
		color: C.green,
		label: 'Brand new tournament',
		action: 'Read it',
		why: 'We have never seen it before.',
	},
	{
		icon: '🔁',
		color: C.yellow,
		label: 'Played in the last 14 days',
		action: 'Read it again',
		why: 'Scores are sometimes fixed a few days late.',
	},
	{
		icon: '⏳',
		color: C.teal,
		label: 'Not finished yet',
		action: 'Keep checking (60 days)',
		why: 'Playoffs or new dates can be added later.',
	},
	{
		icon: '✅',
		color: C.muted,
		label: 'Finished and already saved',
		action: 'Skip',
		why: 'Nothing will change, so no need to ask again.',
	},
];

export const Rules: React.FC = () => {
	const d = DURATIONS.rules;
	return (
		<Scene duration={d}>
			<Heading step="Step 2 · the smart part" title="Which tournaments does it read?" />
			<div
				style={{
					position: 'absolute',
					top: 270,
					left: 110,
					right: 110,
					display: 'flex',
					flexDirection: 'column',
					gap: 22,
				}}
			>
				{rules.map((r, i) => (
					<RuleRow key={r.label} rule={r} delay={30 + i * 90} />
				))}
			</div>
			<Captions
				lines={[
					{
						from: 15,
						text: <>Reading everything every night would be slow, so the robot follows a few simple rules.</>,
					},
					{
						from: 120,
						text: (
							<>
								Recent tournaments are read <Hl color={C.yellow}>again</Hl>, and the old copy is replaced
								with the fresh one.
							</>
						),
					},
					{
						from: 300,
						text: (
							<>
								This way the data stays <Hl>correct</Hl>, while each night stays quick.
							</>
						),
					},
				]}
			/>
		</Scene>
	);
};

const RuleRow: React.FC<{rule: (typeof rules)[number]; delay: number}> = ({rule, delay}) => {
	const p = usePop(delay);
	return (
		<Card
			style={{
				display: 'grid',
				gridTemplateColumns: '90px 1fr 500px',
				alignItems: 'center',
				padding: '20px 34px',
				opacity: p,
				transform: `translateX(${(1 - p) * -80}px)`,
				borderLeft: `10px solid ${rule.color}`,
			}}
		>
			<div style={{fontSize: 56}}>{rule.icon}</div>
			<div>
				<div style={{fontSize: 38, fontWeight: 800}}>{rule.label}</div>
				<div style={{fontSize: 26, color: C.muted, marginTop: 4}}>{rule.why}</div>
			</div>
			<div
				style={{
					justifySelf: 'end',
					fontSize: 32,
					fontWeight: 800,
					color: C.bg,
					background: rule.color,
					padding: '12px 26px',
					borderRadius: 16,
					whiteSpace: 'nowrap',
				}}
			>
				{rule.action}
			</div>
		</Card>
	);
};

/* ------------------------------------------------------------------ */

const people = [
	{name: 'Anna Berg', known: true},
	{name: 'Ole Hansen', known: true},
	{name: 'Mia Lind', known: false},
	{name: 'Jon Dahl', known: true},
];

export const Players: React.FC = () => {
	const d = DURATIONS.players;
	const frame = useCurrentFrame();
	const chips = useProgress(30, 90);
	const check = frame > 120;
	const profile = usePop(190);
	const arrow = useProgress(280, 310);
	const sheet = usePop(300);
	return (
		<Scene duration={d}>
			<Heading step="Step 3" title="Look up new players" />
			<div
				style={{
					position: 'absolute',
					left: 110,
					top: 270,
					width: 560,
					display: 'flex',
					flexDirection: 'column',
					gap: 18,
				}}
			>
				<div style={{fontSize: 28, color: C.muted, fontWeight: 700}}>Names in the match list</div>
				{people.map((p, i) => {
					const show = chips * people.length > i;
					return (
						<div
							key={p.name}
							style={{
								display: 'flex',
								justifyContent: 'space-between',
								alignItems: 'center',
								background: C.card,
								border: `2px solid ${check && !p.known ? C.orange : C.line}`,
								borderRadius: 18,
								padding: '16px 24px',
								fontSize: 32,
								opacity: show ? (check && p.known ? 0.5 : 1) : 0,
							}}
						>
							<span>👤 {p.name}</span>
							{check ? (
								<span
									style={{
										fontSize: 22,
										fontWeight: 800,
										color: p.known ? C.muted : C.orange,
									}}
								>
									{p.known ? 'already known' : 'NEW'}
								</span>
							) : null}
						</div>
					);
				})}
			</div>
			<div
				style={{
					position: 'absolute',
					left: 760,
					top: 260,
					opacity: profile,
					transform: `scale(${0.85 + 0.15 * profile})`,
					transformOrigin: 'top left',
				}}
			>
				<Browser url="sportscorpion.com › player › Mia Lind" width={520} height={430}>
					<div style={{display: 'flex', alignItems: 'center', gap: 16, marginBottom: 14}}>
						<div style={{fontSize: 60}}>🧑</div>
						<div style={{fontSize: 32, fontWeight: 800}}>Mia Lind</div>
					</div>
					{[
						['Country', 'Norway'],
						['City', 'Oslo'],
						['World ranking', 'ID 12345'],
						['Born', '1998'],
					].map(([k, v]) => (
						<div
							key={k}
							style={{
								display: 'flex',
								justifyContent: 'space-between',
								fontSize: 25,
								padding: '10px 0',
								borderTop: '1px solid #ddd',
							}}
						>
							<span style={{color: '#5b6677'}}>{k}</span>
							<b>{v}</b>
						</div>
					))}
				</Browser>
			</div>
			<Arrow x1={1295} y1={470} x2={1370} y2={470} progress={arrow} />
			<div
				style={{
					position: 'absolute',
					left: 1390,
					top: 270,
					opacity: sheet,
					transform: `translateY(${(1 - sheet) * 30}px)`,
				}}
			>
				<Sheet
					title="Player list"
					width={430}
					columns={['Player', 'Country']}
					rows={[
						['Anna Berg', 'Sweden'],
						['Ole Hansen', 'Denmark'],
						['Jon Dahl', 'Norway'],
						['Mia Lind', 'Norway'],
					]}
					highlightLast
				/>
			</div>
			<Captions
				lines={[
					{
						from: 15,
						text: <>Last, the robot goes through every name that appears in the match list.</>,
					},
					{
						from: 120,
						text: (
							<>
								Players it already knows are skipped. For <Hl>new</Hl> players, it opens their profile
								page…
							</>
						),
					},
					{
						from: 260,
						text: <>…and saves their country, city and world-ranking number to the player list.</>,
					},
				]}
			/>
		</Scene>
	);
};

/* ------------------------------------------------------------------ */

export const Polite: React.FC = () => {
	const d = DURATIONS.polite;
	const frame = useCurrentFrame();
	const helpers = 5;
	const retryP = usePop(200);
	// Retry animation: a busy sign, then a wait, then success.
	const retryState = frame < 240 ? 'busy' : frame < 300 ? 'wait' : 'ok';
	return (
		<Scene duration={d}>
			<Heading title="Fast, but polite" color={C.teal} />
			<div
				style={{
					position: 'absolute',
					left: 110,
					top: 260,
					display: 'flex',
					gap: 20,
				}}
			>
				{Array.from({length: helpers}).map((_, i) => {
					const p = usePop(20 + i * 10);
					const progress = interpolate(frame, [40 + i * 8, 170 + i * 12], [0, 1], {
						extrapolateLeft: 'clamp',
						extrapolateRight: 'clamp',
					});
					return (
						<div key={i} style={{width: 150, opacity: p, textAlign: 'center'}}>
							<Robot size={120} />
							<div
								style={{
									height: 16,
									background: C.card,
									borderRadius: 8,
									overflow: 'hidden',
									marginTop: 10,
								}}
							>
								<div
									style={{width: `${progress * 100}%`, height: '100%', background: C.teal}}
								/>
							</div>
						</div>
					);
				})}
			</div>
			<Card
				style={{
					position: 'absolute',
					left: 1050,
					top: 260,
					width: 760,
					padding: 36,
					opacity: retryP,
					transform: `translateY(${(1 - retryP) * 40}px)`,
				}}
			>
				<div style={{fontSize: 34, fontWeight: 800, marginBottom: 20}}>When the website is busy</div>
				<div style={{display: 'flex', gap: 18, alignItems: 'center', fontSize: 30}}>
					<Badge active={retryState === 'busy'} color={C.red} text="🚧 Busy" />
					<span style={{color: C.muted}}>→</span>
					<Badge active={retryState === 'wait'} color={C.yellow} text="⏳ Wait" />
					<span style={{color: C.muted}}>→</span>
					<Badge active={retryState === 'ok'} color={C.green} text="✅ Try again" />
				</div>
				<div style={{fontSize: 26, color: C.muted, marginTop: 22, lineHeight: 1.4}}>
					It waits a little longer each time, and tries up to 5 times before giving up on that page.
				</div>
			</Card>
			<Captions
				lines={[
					{
						from: 15,
						text: (
							<>
								To save time, several <Hl color={C.teal}>helpers</Hl> work side by side, each reading a
								different page.
							</>
						),
					},
					{
						from: 110,
						text: <>But only a handful at once, so the website isn't overloaded.</>,
					},
					{
						from: 200,
						text: (
							<>
								If the site is slow or busy, the robot simply <Hl color={C.yellow}>waits</Hl> and tries
								again.
							</>
						),
					},
				]}
			/>
		</Scene>
	);
};

const Badge: React.FC<{active: boolean; color: string; text: string}> = ({active, color, text}) => (
	<div
		style={{
			padding: '12px 20px',
			borderRadius: 14,
			fontWeight: 800,
			background: active ? color : 'transparent',
			color: active ? C.bg : C.muted,
			border: `3px solid ${active ? color : C.line}`,
		}}
	>
		{text}
	</div>
);

/* ------------------------------------------------------------------ */

// Row counts from the data files in the repo (October 2026).
const stats = [
	{icon: '🏆', value: 6772, label: 'tournaments'},
	{icon: '🏒', value: 1017216, label: 'matches'},
	{icon: '👤', value: 10496, label: 'players'},
];

export const Saved: React.FC = () => {
	const d = DURATIONS.saved;
	const count = useProgress(90, 220);
	const eased = 1 - Math.pow(1 - count, 3);
	return (
		<Scene duration={d}>
			<Heading title="Everything is saved" />
			<div
				style={{
					position: 'absolute',
					top: 300,
					left: 0,
					right: 0,
					display: 'flex',
					justifyContent: 'center',
					gap: 50,
				}}
			>
				{stats.map((s, i) => (
					<StatCard key={s.label} {...s} value={Math.round(s.value * eased)} delay={20 + i * 20} />
				))}
			</div>
			<Captions
				lines={[
					{
						from: 15,
						text: (
							<>
								When it's done, the robot saves the updated files back into the project on{' '}
								<Hl>GitHub</Hl>.
							</>
						),
					},
					{
						from: 120,
						text: (
							<>
								Night after night, the collection grows. Right now it holds results going back to{' '}
								<Hl>2010</Hl>.
							</>
						),
					},
				]}
			/>
		</Scene>
	);
};

const StatCard: React.FC<{icon: string; value: number; label: string; delay: number}> = ({
	icon,
	value,
	label,
	delay,
}) => {
	const p = usePop(delay);
	return (
		<Card
			style={{
				width: 520,
				padding: '40px 30px',
				textAlign: 'center',
				opacity: p,
				transform: `scale(${0.8 + 0.2 * p})`,
			}}
		>
			<div style={{fontSize: 70}}>{icon}</div>
			<div
				style={{
					fontSize: 84,
					fontWeight: 900,
					color: C.orange,
					fontVariantNumeric: 'tabular-nums',
					marginTop: 10,
				}}
			>
				{value.toLocaleString('en-US')}
			</div>
			<div style={{fontSize: 36, color: C.muted, fontWeight: 600}}>{label}</div>
		</Card>
	);
};

/* ------------------------------------------------------------------ */

const recap = [
	['🌙', 'Wakes up at midnight'],
	['🏆', 'Finds new tournaments'],
	['🏒', 'Reads their matches'],
	['👤', 'Looks up new players'],
	['💾', 'Saves everything'],
];

export const Recap: React.FC = () => {
	const d = DURATIONS.recap;
	const end = usePop(250);
	return (
		<Scene duration={d}>
			<Heading title="In short" />
			<div
				style={{
					position: 'absolute',
					top: 290,
					left: 0,
					right: 0,
					display: 'flex',
					justifyContent: 'center',
					gap: 26,
				}}
			>
				{recap.map(([icon, text], i) => (
					<RecapItem key={text} icon={icon} text={text} delay={20 + i * 30} n={i + 1} />
				))}
			</div>
			<div
				style={{
					position: 'absolute',
					top: 680,
					left: 0,
					right: 0,
					textAlign: 'center',
					fontSize: 52,
					fontWeight: 800,
					opacity: end,
					transform: `translateY(${(1 - end) * 30}px)`,
				}}
			>
				…and tomorrow night, it does it all <Hl>again</Hl>. 🔁
			</div>
		</Scene>
	);
};

const RecapItem: React.FC<{icon: string; text: string; delay: number; n: number}> = ({
	icon,
	text,
	delay,
	n,
}) => {
	const p = usePop(delay);
	return (
		<Card
			style={{
				width: 300,
				height: 300,
				padding: 26,
				display: 'flex',
				flexDirection: 'column',
				alignItems: 'center',
				justifyContent: 'center',
				textAlign: 'center',
				opacity: p,
				transform: `translateY(${(1 - p) * 50}px)`,
			}}
		>
			<div style={{fontSize: 24, color: C.orange, fontWeight: 800}}>{n}</div>
			<div style={{fontSize: 80, margin: '8px 0 14px'}}>{icon}</div>
			<div style={{fontSize: 32, fontWeight: 700, lineHeight: 1.25}}>{text}</div>
		</Card>
	);
};
