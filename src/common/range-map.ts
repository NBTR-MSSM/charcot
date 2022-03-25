import { Range, RangeInfo } from '../types/charcot.types'

/**
 * Specialized map that when queried for a numeric value it actually gives a range
 * to which such value belongs, for example given value '4' this map's get()
 * method reeturns range 4 - 6.
 * The range interval, the min and max ranges are calculated based on
 * the input arguments to the constructor. See the constructor documentation
 * for more info.
 *
 * interval = 6, max = 12, start = 6
 * X,  0 => < 6
 * 6,  1 => 06 - 11
 * 12, 2 => 12 - 17
 * 18, 3 => 18 >
 *
 * interval = 6, max = 24, start = 12
 * X,  0 => < 12
 * 12, 1 => 12 - 17
 * 18, 2 => 18 - 23
 * 24, 3 => 24 >
 */
export default class RangeMap {
  private readonly interval: number
  private readonly max: number
  private readonly start: number
  private readonly map: Map<number, Range> = new Map<number, Range>()
  private readonly rangeIndexAdjustment: number

  /**
   * Initializes a RangeMap object.
   *
   * @param interval - How big each range should be
   * @param max - The max value for which there's a range, for example if 90 then numbers higher than that fall into the > 90 range
   * @param start - The number at which to start ranges, for example if 6, numbers less than that just fall into the < 6 range
   */
  constructor(interval: number, max: number, start: number) {
    this.interval = interval
    this.max = max
    this.start = start
    this.rangeIndexAdjustment = this.start !== this.interval ? (this.start / this.interval) - 1 : 0
    this.map.set(0, `< ${start}`)
    let cur = start

    while (cur < this.max) {
      this.map.set(this.calculateRangeIndex(cur), `${cur} - ${(cur += this.interval) - 1}`)
    }
    this.map.set(this.calculateRangeIndex(cur), `${cur} <=`)
  }

  get(val: number): RangeInfo | undefined {
    let idx = this.calculateRangeIndex(val)
    // Dimension values outside of the max fall into the max bucket (the last range).
    const range = this.map.get(idx) || ((idx = this.calculateRangeIndex(this.max)) && this.map.get(idx))
    return { range: range as Range, index: idx }
  }

  private calculateRangeIndex(val: number): number {
    const idx = Math.floor((val / this.interval) - this.rangeIndexAdjustment)
    // The adjustment applied when generating the ranges in the constructors
    // results in negative index when the passed in value is less than the interval,
    // ceiling those to '0'
    return idx < 0 ? 0 : idx
  }
}
