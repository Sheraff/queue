import test from "node:test"
import { parseDuration, parsePeriod } from '../src/ms'
import assert from "node:assert"

test.describe('parseDuration', () => {
	test('narrow', () => {
		assert.equal(parseDuration('100ms'), 100, 'miliseconds')
		assert.equal(parseDuration('2s'), 2_000, 'seconds')
		assert.equal(parseDuration('3m'), 180_000, 'minutes')
		assert.equal(parseDuration('4h'), 14_400_000, 'hours')
		assert.equal(parseDuration('5d'), 432_000_000, 'days')
		assert.equal(parseDuration('6w'), 3_628_800_000, 'weeks')
		assert.equal(parseDuration('7y'), 220_903_200_000, 'years')
	})
	test('short', () => {
		assert.equal(parseDuration('100 msecs'), 100, 'miliseconds')
		assert.equal(parseDuration('2 sec'), 2_000, 'seconds')
		assert.equal(parseDuration('3 min'), 180_000, 'minutes')
		assert.equal(parseDuration('4hrs'), 14_400_000, 'hours')
		assert.equal(parseDuration('5 day'), 432_000_000, 'days')
		assert.equal(parseDuration('6 week'), 3_628_800_000, 'weeks')
		assert.equal(parseDuration('7 yrs'), 220_903_200_000, 'years')
	})
	test('long', () => {
		assert.equal(parseDuration('100 milliseconds'), 100, 'miliseconds')
		assert.equal(parseDuration('2 seconds'), 2_000, 'seconds')
		assert.equal(parseDuration('3 minutes'), 180_000, 'minutes')
		assert.equal(parseDuration('4 hours'), 14_400_000, 'hours')
		assert.equal(parseDuration('5 days'), 432_000_000, 'days')
		assert.equal(parseDuration('6 weeks'), 3_628_800_000, 'weeks')
		assert.equal(parseDuration('7 years'), 220_903_200_000, 'years')
	})
	assert.equal(parseDuration('100'), 100, 'no unit')
	assert.equal(parseDuration('100.5'), 100.5, 'float')
	assert.equal(parseDuration('100ms'), 100, 'no unit')
	assert.equal(parseDuration('100.5ms'), 100.5, 'float')
})

test.describe('parseFrequency', () => {
	test('narrow', () => {
		assert.equal(parsePeriod('5 / ms'), 0.2, 'miliseconds')
		assert.equal(parsePeriod('2 / s'), 500, 'seconds')
		assert.equal(parsePeriod('3 per m'), 20_000, 'minutes')
		assert.equal(parsePeriod('4/h'), 900_000, 'hours')
		assert.equal(parsePeriod('5/d'), 17_280_000, 'days')
		assert.equal(parsePeriod('6/w'), 100_800_000, 'weeks')
		assert.equal(parsePeriod('8/y'), 3_944_700_000, 'years')
	})
	test('short', () => {
		assert.equal(parsePeriod('5 per ms'), 0.2, 'miliseconds')
		assert.equal(parsePeriod('2/sec'), 500, 'seconds')
		assert.equal(parsePeriod('3 per min'), 20_000, 'minutes')
		assert.equal(parsePeriod('4 per hr'), 900_000, 'hours')
		assert.equal(parsePeriod('5 per day'), 17_280_000, 'days')
		assert.equal(parsePeriod('6 / week'), 100_800_000, 'weeks')
		assert.equal(parsePeriod('8 per yr'), 3_944_700_000, 'years')
	})
	test('long', () => {
		assert.equal(parsePeriod('5 per millisecond'), 0.2, 'miliseconds')
		assert.equal(parsePeriod('2 per second'), 500, 'seconds')
		assert.equal(parsePeriod('3 per minute'), 20_000, 'minutes')
		assert.equal(parsePeriod('4 per hour'), 900_000, 'hours')
		assert.equal(parsePeriod('5 per day'), 17_280_000, 'days')
		assert.equal(parsePeriod('6 per weeks'), 100_800_000, 'weeks')
		assert.equal(parsePeriod('8/year'), 3_944_700_000, 'years')
	})
})
