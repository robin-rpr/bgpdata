/**
 * Translates given line segment such that the distance between the given line segment and the translated line segment equals targetDistance.
 * @param {number} distance The distance between the segment and the origin position.
 * @param {x,y} pointA First point that defines the line segment.
 * @param {x,y} pointB Second point that defines the line segment.
 * @link https://bl.ocks.org/ramtob/3658a11845a89c4742d62d32afce3160
 * @returns {Object} Translation Vector
 */
export function translate(distance, pointA, pointB) {
    const x1x0 = pointB.x - pointA.x;
    const y1y0 = pointB.y - pointA.y;
    let x2x0, y2y0;

    if (y1y0 === 0) {
        x2x0 = 0;
        y2y0 = distance;
    } else {
        const angle = Math.atan(x1x0 / y1y0);
        x2x0 = -distance * Math.cos(angle);
        y2y0 = distance * Math.sin(angle);
    }
    return {
        dx: x2x0,
        dy: y2y0,
    };
}