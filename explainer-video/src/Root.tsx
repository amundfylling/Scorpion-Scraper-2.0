import React from 'react';
import {Composition, Series} from 'remotion';
import {
	DURATIONS,
	Intro,
	Matches,
	Nightly,
	Players,
	Polite,
	Problem,
	Recap,
	Rules,
	Saved,
	Tournaments,
} from './scenes';

const SCENES: [keyof typeof DURATIONS, React.FC][] = [
	['intro', Intro],
	['problem', Problem],
	['nightly', Nightly],
	['tournaments', Tournaments],
	['matches', Matches],
	['rules', Rules],
	['players', Players],
	['polite', Polite],
	['saved', Saved],
	['recap', Recap],
];

const TOTAL = SCENES.reduce((sum, [key]) => sum + DURATIONS[key], 0);

const ScraperExplainer: React.FC = () => (
	<Series>
		{SCENES.map(([key, SceneComponent]) => (
			<Series.Sequence key={key} durationInFrames={DURATIONS[key]}>
				<SceneComponent />
			</Series.Sequence>
		))}
	</Series>
);

export const RemotionRoot: React.FC = () => (
	<Composition
		id="ScraperExplainer"
		component={ScraperExplainer}
		durationInFrames={TOTAL}
		fps={30}
		width={1920}
		height={1080}
	/>
);
