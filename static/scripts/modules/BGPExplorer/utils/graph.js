/*** 
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from Route Collectors around the world.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
***/

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