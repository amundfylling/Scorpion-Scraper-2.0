import React from 'react';
import {AbsoluteFill, interpolate, spring, useCurrentFrame, useVideoConfig} from 'remotion';

export const C = {
	bg: '#0e1a2b',
	bg2: '#14243a',
	card: '#1b2f4a',
	cardLight: '#24406380',
	line: '#2f4b70',
	text: '#f3f6fb',
	muted: '#9fb3cc',
	orange: '#ff8a3d',
	teal: '#3dd6c6',
	green: '#4ade80',
	red: '#f87171',
	yellow: '#facc15',
	paper: '#f7f4ec',
	ink: '#1d2633',
};

export const FONT = 'Inter, "Noto Color Emoji", sans-serif';

/** Spring from 0 to 1 starting at `delay` frames. */
export const usePop = (delay = 0, damping = 14) => {
	const frame = useCurrentFrame();
	const {fps} = useVideoConfig();
	return spring({frame: frame - delay, fps, config: {damping}});
};

/** Linear 0 -> 1 between two frames, clamped. */
export const useProgress = (from: number, to: number) => {
	const frame = useCurrentFrame();
	return interpolate(frame, [from, to], [0, 1], {
		extrapolateLeft: 'clamp',
		extrapolateRight: 'clamp',
	});
};

export const Scene: React.FC<{duration: number; children: React.ReactNode}> = ({
	duration,
	children,
}) => {
	const frame = useCurrentFrame();
	const opacity = interpolate(frame, [0, 12, duration - 12, duration], [0, 1, 1, 0], {
		extrapolateLeft: 'clamp',
		extrapolateRight: 'clamp',
	});
	return (
		<AbsoluteFill
			style={{
				background: `radial-gradient(circle at 30% 20%, ${C.bg2} 0%, ${C.bg} 70%)`,
				fontFamily: FONT,
				color: C.text,
			}}
		>
			<AbsoluteFill style={{opacity}}>{children}</AbsoluteFill>
		</AbsoluteFill>
	);
};

/** Small step label + heading at the top of a scene. */
export const Heading: React.FC<{step?: string; title: string; color?: string}> = ({
	step,
	title,
	color = C.orange,
}) => {
	const p = usePop(0);
	return (
		<div
			style={{
				position: 'absolute',
				top: 70,
				left: 110,
				right: 110,
				opacity: p,
				transform: `translateY(${(1 - p) * -30}px)`,
			}}
		>
			{step ? (
				<div
					style={{
						color,
						fontSize: 30,
						fontWeight: 700,
						letterSpacing: 4,
						textTransform: 'uppercase',
						marginBottom: 8,
					}}
				>
					{step}
				</div>
			) : null}
			<div style={{fontSize: 64, fontWeight: 800, lineHeight: 1.1}}>{title}</div>
		</div>
	);
};

/**
 * Narration shown at the bottom of the screen. Each line takes over from the
 * previous one at its `from` frame.
 */
export const Captions: React.FC<{lines: {from: number; text: React.ReactNode}[]}> = ({lines}) => {
	const frame = useCurrentFrame();
	const index = lines.reduce((acc, line, i) => (frame >= line.from ? i : acc), -1);
	if (index < 0) {
		return null;
	}
	const line = lines[index];
	const local = frame - line.from;
	const opacity = interpolate(local, [0, 10], [0, 1], {extrapolateRight: 'clamp'});
	return (
		<div
			style={{
				position: 'absolute',
				left: 0,
				right: 0,
				bottom: 60,
				display: 'flex',
				justifyContent: 'center',
			}}
		>
			<div
				style={{
					opacity,
					transform: `translateY(${(1 - opacity) * 14}px)`,
					background: 'rgba(5, 12, 22, 0.78)',
					border: `2px solid ${C.line}`,
					borderRadius: 22,
					padding: '22px 40px',
					maxWidth: 1500,
					fontSize: 40,
					fontWeight: 500,
					lineHeight: 1.35,
					textAlign: 'center',
				}}
			>
				{line.text}
			</div>
		</div>
	);
};

export const Hl: React.FC<{children: React.ReactNode; color?: string}> = ({
	children,
	color = C.orange,
}) => <span style={{color, fontWeight: 800}}>{children}</span>;

export const Card: React.FC<{
	style?: React.CSSProperties;
	children: React.ReactNode;
}> = ({style, children}) => (
	<div
		style={{
			background: C.card,
			border: `2px solid ${C.line}`,
			borderRadius: 24,
			boxShadow: '0 20px 50px rgba(0,0,0,0.35)',
			...style,
		}}
	>
		{children}
	</div>
);

/** A cartoon browser window, used for "a page on the website". */
export const Browser: React.FC<{
	url: string;
	width: number;
	height: number;
	style?: React.CSSProperties;
	children: React.ReactNode;
}> = ({url, width, height, style, children}) => (
	<div
		style={{
			width,
			height,
			background: C.paper,
			borderRadius: 18,
			overflow: 'hidden',
			boxShadow: '0 24px 60px rgba(0,0,0,0.45)',
			color: C.ink,
			display: 'flex',
			flexDirection: 'column',
			...style,
		}}
	>
		<div
			style={{
				height: 54,
				background: '#dfe4ec',
				display: 'flex',
				alignItems: 'center',
				padding: '0 18px',
				gap: 10,
				flexShrink: 0,
			}}
		>
			{['#f87171', '#facc15', '#4ade80'].map((c) => (
				<div key={c} style={{width: 16, height: 16, borderRadius: 8, background: c}} />
			))}
			<div
				style={{
					marginLeft: 12,
					flex: 1,
					background: 'white',
					borderRadius: 10,
					padding: '6px 16px',
					fontSize: 22,
					color: '#5b6677',
					whiteSpace: 'nowrap',
					overflow: 'hidden',
				}}
			>
				{url}
			</div>
		</div>
		<div style={{flex: 1, padding: 24, position: 'relative'}}>{children}</div>
	</div>
);

/** The friendly robot that stands in for the scraper. */
export const Robot: React.FC<{size?: number; style?: React.CSSProperties; blink?: boolean}> = ({
	size = 200,
	style,
}) => {
	const frame = useCurrentFrame();
	const eyesClosed = frame % 90 > 84;
	const bob = Math.sin(frame / 10) * 4;
	return (
		<svg
			width={size}
			height={size * 1.15}
			viewBox="0 0 200 230"
			style={{transform: `translateY(${bob}px)`, ...style}}
		>
			<line x1="100" y1="10" x2="100" y2="40" stroke={C.orange} strokeWidth="6" />
			<circle cx="100" cy="10" r="10" fill={C.orange} />
			<rect x="30" y="40" width="140" height="110" rx="30" fill={C.teal} />
			<rect x="48" y="62" width="104" height="62" rx="18" fill={C.bg} />
			{eyesClosed ? (
				<>
					<rect x="68" y="90" width="22" height="5" rx="2" fill={C.teal} />
					<rect x="110" y="90" width="22" height="5" rx="2" fill={C.teal} />
				</>
			) : (
				<>
					<circle cx="79" cy="92" r="11" fill={C.teal} />
					<circle cx="121" cy="92" r="11" fill={C.teal} />
				</>
			)}
			<rect x="55" y="158" width="90" height="62" rx="18" fill={C.teal} />
			<rect x="80" y="175" width="40" height="10" rx="5" fill={C.bg} />
			<rect x="10" y="165" width="40" height="16" rx="8" fill={C.teal} />
			<rect x="150" y="165" width="40" height="16" rx="8" fill={C.teal} />
		</svg>
	);
};

/** A simple spreadsheet-looking table, used for "a saved data file". */
export const Sheet: React.FC<{
	title: string;
	columns: string[];
	rows: string[][];
	visibleRows?: number;
	width?: number;
	highlightLast?: boolean;
	style?: React.CSSProperties;
}> = ({title, columns, rows, visibleRows, width = 760, highlightLast, style}) => {
	const shown = rows.slice(0, visibleRows ?? rows.length);
	return (
		<div style={{width, ...style}}>
			<div style={{display: 'flex', alignItems: 'center', gap: 14, marginBottom: 12}}>
				<div style={{fontSize: 38}}>📄</div>
				<div style={{fontSize: 30, fontWeight: 700}}>{title}</div>
			</div>
			<div
				style={{
					background: 'white',
					color: C.ink,
					borderRadius: 14,
					overflow: 'hidden',
					boxShadow: '0 18px 40px rgba(0,0,0,0.35)',
				}}
			>
				<div
					style={{
						display: 'grid',
						gridTemplateColumns: `repeat(${columns.length}, 1fr)`,
						background: '#2b8a7f',
						color: 'white',
						fontWeight: 700,
						fontSize: 22,
					}}
				>
					{columns.map((c) => (
						<div key={c} style={{padding: '12px 14px'}}>
							{c}
						</div>
					))}
				</div>
				{shown.map((row, i) => (
					<div
						key={i}
						style={{
							display: 'grid',
							gridTemplateColumns: `repeat(${columns.length}, 1fr)`,
							fontSize: 22,
							borderTop: '1px solid #e3e8ef',
							background:
								highlightLast && i === shown.length - 1 ? '#fff4d6' : i % 2 ? '#f6f8fb' : 'white',
						}}
					>
						{row.map((cell, j) => (
							<div
								key={j}
								style={{
									padding: '11px 14px',
									whiteSpace: 'nowrap',
									overflow: 'hidden',
									textOverflow: 'ellipsis',
								}}
							>
								{cell}
							</div>
						))}
					</div>
				))}
			</div>
		</div>
	);
};

export const Arrow: React.FC<{
	x1: number;
	y1: number;
	x2: number;
	y2: number;
	progress: number;
	color?: string;
}> = ({x1, y1, x2, y2, progress, color = C.orange}) => {
	const x = x1 + (x2 - x1) * progress;
	const y = y1 + (y2 - y1) * progress;
	const angle = Math.atan2(y2 - y1, x2 - x1);
	const head = 22;
	return (
		<svg style={{position: 'absolute', inset: 0, overflow: 'visible'}} width="1920" height="1080">
			<line
				x1={x1}
				y1={y1}
				x2={x}
				y2={y}
				stroke={color}
				strokeWidth={8}
				strokeLinecap="round"
				strokeDasharray="2 18"
			/>
			{progress > 0.05 ? (
				<polygon
					points={`${x},${y} ${x - head * Math.cos(angle - 0.5)},${y - head * Math.sin(angle - 0.5)} ${
						x - head * Math.cos(angle + 0.5)
					},${y - head * Math.sin(angle + 0.5)}`}
					fill={color}
				/>
			) : null}
		</svg>
	);
};
