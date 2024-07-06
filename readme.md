# reproject grid to h3 with interpolation

one day it would be nice to automate this enough to go from a table of `midpoint | area | value` to `h3 | value` with appropriate ring sizes etc. picked

at the moment it is not that

# caveats

some accuracy will be lost as hexagons are not rectangular. but h3 are useful enough that i personally don't care

# pic


<p align="center">
<img src="assets/nice_map.png" alt="Map of geostat 2011 population grid reprojected to H3 hexagons over Nice">
Geostat population 1km^2 grid reprojected to H3 hexagons, coloured by quantile of population living there. Quantiles with are calculated with respect to cumulative sum of population, not with respect to land area. This is a hobby-horse of mine. No-one cares what the median piece of land looks like but that's what everyone calculates all the time.
</p>
