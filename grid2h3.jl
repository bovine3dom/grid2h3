# nb: use julia +1.9.4 to use H3 v0.2.0
using Shapefile, DataFrames, Statistics, RollingFunctions, Proj, ThreadsX, StatsBase, CSV
import H3
import H3.API: kRing, geoToH3, GeoCoord, h3ToParent, hexAreaKm2


"Add [column]_quantile to a dataframe"
addquantiles!(df, column) = begin
    l = size(df,1)
    tdf = copy(df[!, [column]])
    tdf.id = 1:l
    sort!(tdf, column)
    tdf.q = (1:l)./l
    sort!(tdf, :id)
    df[!, Symbol(string(column) * "_quantile")] = tdf.q
end

table = Shapefile.Table("./data/JRC_POPULATION_2018.shp")

df = DataFrame(table)

# geostat is CRS3035

eu2latlon = Proj.Transformation("EPSG:3035", "EPSG:4326")

# eu2latlon(3215009.376512329, 3597518.130827671) # output is definitely lat/lon

point2latlon(p) = eu2latlon(p.y, p.x)
points2mid(ps) = (getfield.(ps, 1), getfield.(ps, 2)) .|> mean

df.midpoint = ThreadsX.map(ps -> points2mid(point2latlon.(ps)), getfield.(df[!, :geometry], :points)) # threadsx was overkill for this it was stupid quick

stripe_number = 2
stripe_size = 300_000
df3 = df#[df.CNTR_ID .== "UK", :]
#mini_df = copy(df[end-(stripe_size*stripe_number):end-((stripe_number-1)*stripe_size), [:midpoint, :TOT_P_2018, :OBJECTID]]) # 300_000 is hard for computer :(
mini_df = copy(df3[!, [:midpoint, :TOT_P_2018, :OBJECTID, :CNTR_ID]]) 

# resolution 9: at distance 2 it covers about 2km^2
rings = mapreduce(k -> map(midpoint -> (OBJECTID=midpoint[2], h3=kRing(geoToH3(GeoCoord(deg2rad.(midpoint[1])...), 9), k), dist=k+1), zip(mini_df.midpoint, mini_df.OBJECTID)), vcat, 0:2) |> DataFrame
rings = flatten(rings, :h3)

# do it the "right way"
#combine(mini_df, :midpoint => x -> map(p -> (h3=kRing(geoToH3(GeoCoord(deg2rad.(p)...), 9), 2),), x))
#mapreduce(k -> combine(mini_df, AsTable(:) => ByRow(row->(OBJECTID=row.OBJECTID,h3=kRing(geoToH3(GeoCoord(deg2rad.(row.midpoint)...), 9), k), dist=k+1,)) => AsTable), vcat, 0:2) # after all that hassle the one above is quicker and does the same thing, story of my life


# sort!(rings, :dist) # comes presorted
rings = rings[.!nonunique(rings, [:h3, :OBJECTID], keep=:first), :]
leftjoin!(rings, mini_df[!, [:TOT_P_2018, :OBJECTID, :CNTR_ID]], on=:OBJECTID)

# weighted average with centre = 1, nthring = 1/dist^3
df2 = combine(groupby(rings, :h3), [:dist, :TOT_P_2018] => ((d, p) -> mean(p, weights(1 ./ (d.^3)))) => :TOT_P_2018, :CNTR_ID => first => :CNTR_ID)

df2.res .= 9
df2.h3_7 = h3ToParent.(df2.h3, 7)
df2.h3_5 = h3ToParent.(df2.h3, 5)

# i think we need to have a total_pop field which is population density * area
# then here we still take mean population density? not sure. but we need some kind of population density
# but definitely sum total_pop
# then on the frontend we weight all our medians by population and not by density
h3_7s = combine(groupby(df2, :h3_7), :TOT_P_2018 => mean => :TOT_P_2018, :res => (x->7) => :res, :CNTR_ID => first => :CNTR_ID)
rename!(h3_7s, :h3_7 => :h3)
h3_5s = combine(groupby(df2, :h3_5), :TOT_P_2018 => mean => :TOT_P_2018, :res => (x->5) => :res, :CNTR_ID => first => :CNTR_ID)
rename!(h3_5s, :h3_5 => :h3)

df4 = vcat(df2[!, [:h3, :TOT_P_2018, :res, :CNTR_ID]], h3_7s, h3_5s)


# gridding ends, this is just for fun / plotting


df4.index = string.(df4.h3, base=16)
# addquantiles!(df4, :TOT_P_2018)
# df4.value = df4.TOT_P_2018_quantile
# CSV.write("$(homedir())/projects/H3-MON/www/data/h3_data.csv", df4[!, [:index, :value]])

# but those are quantiles based on land, to be based on people we need cumsum
sort!(df4, :TOT_P_2018)
df4.pop_quantile = cumsum(df4.TOT_P_2018) ./ sum(df4.TOT_P_2018)
df4.value = df4.pop_quantile
df4.real_value = df4.TOT_P_2018
# CSV.write("$(homedir())/projects/H3-MON/www/data/h3_data.csv", df4[!, [:index, :value, :real_value]]) # could cut off e.g. everything below 5th percentile density to massively reduce file size without losing too much info

# using DuckDB
# # COPY orders TO 'orders' (FORMAT PARQUET, PARTITION_BY (year, month));
# con = DBInterface.connect(DuckDB.DB)
# DuckDB.register_data_frame(con, df4, "df4")
# # results = DBInterface.execute(con, "SELECT count(*) FROM df4")
# diditwork = DBInterface.execute(con, "COPY df4 TO 'JRC_POPULATION_2018_H3' (FORMAT PARQUET, PARTITION_BY (res, CNTR_ID))")

# using DuckDB, DataFrames
# df4 = DBInterface.execute(con, "SELECT * from '../H3-MON/www/data/JRC_POPULATION_2018_H3/**/*.parquet'") |> DataFrame

using ArrowHivePartitioner
using Arrow, DataFrames
df4.res = string.(df4.res)
writehivedir("JRC_POPULATION_2018_H3", df4, [:res, :CNTR_ID])

df4 = readhivedir("../H3-MON/www/data/JRC_POPULATION_2018_H3")
select!(df4, [:index, :value, :real_value, :res, :CNTR_ID])
df4.h3_3 = string.(h3ToParent.(parse.(UInt64, df4.index, base=16), 3), base=16)

# arquero doesn't support zstd
function writehivedir(outdir, df, groupkeys=[]; filename="part0.arrow")
    g = groupby(df, groupkeys)
    for t in keys(g)
        !all(v -> >:(AbstractString, typeof(v)), values(t)) && throw("All grouped column values must be strings") # TODO: support other types?
        path = join(["$k=$v" for (k,v) in zip(keys(t), values(t))], "/")
        mkpath(joinpath(outdir,path))
        Arrow.write(joinpath(outdir,path,filename), g[t][!, Not(keys(t))])
    end
end
writehivedir("../H3-MON/www/data/JRC_POPULATION_2018_H3", df4, [:res, :CNTR_ID])
writehivedir("../H3-MON/www/data/JRC_POPULATION_2018_H3_by_rnd", df4, [:res, :h3_3])

# do i need this?
function densityatquantile(q,poptuple)
    populations, cum_population = poptuple
    quantileth_man(q) = last(cum_population)*q

    quantileth_density(q) = populations[findfirst(x->x>quantileth_man(q), cum_population)]
    quantileth_density(q)
end


# get valid parents to avoid 404
df4 = readhivedir("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/")

using JSON
dfo = combine(groupby(df4, :res), df -> begin
        Ref(unique(df.h3_parent))
        # Ref(unique(df.h3_3))
    end)

# want to aim for ~30 per region
# find that (it will be constant right? kRing of radius n has constant size. 
#   so i guess what we need to know is what distance we will be viewing at
#   and from that find the radius in km of the disk
#   to then find the radius in h3 tiles @ what h3 resolution is ~approx 30
# )
# do line :38 again
# then store in metadata with "chunk_size" => (res => [parent res, radius])

using Interpolations
"Get actual-value scale from normalised values, fudging 0/1"
function dictscale(df, normalised, actual)
    d = @view df[.!nonunique(df, normalised, keep=:first), [normalised, actual]]
    sort!(d, normalised)
    f = linear_interpolation(d[!,normalised], d[!,actual], extrapolation_bc=Flat())
    Dict("scale" => Dict(zip([0, 0.2, 0.4, 0.6, 0.8, 1], round.(f.([0.0001, 0.2, 0.4, 0.6, 0.8, 0.9999]), sigdigits=2))))
end

using Random: shuffle
write("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/meta.json", JSON.json(merge(Dict("valid_parents" => Dict(eachrow(dfo))), dictscale(df4[shuffle(1:nrow(df4))[1:100000], :], :value, :real_value))))


# using H3.Lib
# verts = [Lib.GeoCoord(0,50), Lib.GeoCoord(-1, 50), Lib.GeoCoord(0, 51)]
# geofence = Lib.Geofence(
#     Cint(3),
#     pointer(verts),
# )
# poly = Lib.GeoPolygon(geofence, Cint(0), pointer([]))
# Lib.polyfill
# len = Lib.maxPolyfillSize(poly, 7)
# h3s = Vector{Lib.H3Index}(undef, len)



# tiff2arrow
using GeoArrays, Proj, Arrow, DataFrames
mollweide2latlon = Proj.Transformation("ESRI:54009", "EPSG:4326", always_xy=true) # always_xy => lon/lat order
# ghs = GeoArrays.read("ghs/GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.tif", masked=false) # masked=false memmaps it
ghs = GeoArrays.read("ghsl_1km/GHS_POP_E2025_GLOBE_R2023A_54009_1000_V1_0.tif", masked=false) # 1km rather than 100x100
filter(i -> ghs[i] >= 0, Iterators.partition(eachindex(ghs), 1000*1000) |> first)

#using ThreadsX
s = size(ghs)[1:2]
a = 1:20000:s[1] |> collect
b =  1:20000:s[2] |> collect
xs = map(z -> range(z...), zip(a[1:end-1], a[2:end].-1))# |> collect # misses off the very row/col but who cares
ys = map(z -> range(z...), zip(b[1:end-1], b[2:end].-1))# |> collect # misses off the very row/col but who cares

coords = Iterators.product(xs,ys) |> collect

using ProgressMeter
using Dates
@showprogress for (i,c) in enumerate(coords) # is @showprogress forcing it to be single threaded? no, something else
    this_ghs = ghs[c...]
    nonzeroes = findall(p -> p > 0.0, this_ghs) # exclude both no data and true zeroes 
    length(nonzeroes) == 0 && continue
    df = DataFrame(map(c -> (mollweide2latlon(GeoArrays.coords(this_ghs, c.I[1:2]))..., this_ghs[c]), nonzeroes), [:lon, :lat, :pop]) # about 100 seconds per coordinate partition
    Arrow.write("arrow_nocompress/part$i.arrow", df)#, compress=:zstd) # pretty sure compression means we can't memory map
end
# for 1km squares
nonzeroes = findall(p -> p > 0.0, ghs) # exclude both no data and true zeroes 
df = DataFrame(map(c -> (mollweide2latlon(GeoArrays.coords(ghs, c.I[1:2]))..., ghs[c]), nonzeroes), [:lon, :lat, :pop]) # about 100 seconds per coordinate partition
Arrow.write("ghs_1km.arrow", df)#, compress=:zstd) # pretty sure compression means we can't memory map

# gdal_translate -of XYZ GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.tif ghs.csv # single threaded but better? who knows

# gdal_translate -of XYZ GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.tif /vsistdout | awk '$3 != -200' > ghs.csv # skip nodata,
# absurdly slow (ran for four hours and got ~1/4 of the way through), julia does it in 20 minutes

# https://human-settlement.emergency.copernicus.eu/download.php?ds=pop - no real reason why we have to do 100x100m grid to get an MVP out, start with 1km and it'll be digestable



# conversion to h3
using Tables, TableOperations, Arrow, DataFrames, StatsBase, ThreadsX
import H3.API: kRing, geoToH3, GeoCoord, h3ToParent
df = Tables.partitioner(Arrow.Table, "arrow/".*readdir("arrow/")) |> TableOperations.joinpartitions |> DataFrame # to mmap it _must not_ be compressed (disk based is fine)
GC.gc()
df = DataFrame(Arrow.Table("arrow/".*readdir("arrow/")[10])) # [10] is ~fiji
# df = DataFrame(Arrow.Table("arrow/part51.arrow")) # too big

df.h3 = geoToH3.(GeoCoord.(deg2rad.(df.lat), deg2rad.(df.lon)), 11) # h3 11 approx right res for 100m^2
df.h3 = ThreadsX.map(p -> geoToH3(GeoCoord(deg2rad(p[1]), deg2rad(p[2])), 11), zip(df.lat, df.lon)) # ok but once we do this what is the next step, like, where are you going to write it?

# bug: lone 100m squares surrounded by 0 population get made bigger
rings = flatten(DataFrame(mapreduce(k->map(h -> (centre=h, h3=kRing(h, k), dist=k+1,), df.h3), vcat, 0:2)), :h3) # slow - multithread?
leftjoin!(rings, df, on=:centre=>:h3)

df2 = combine(groupby(rings, :h3), [:dist, :pop] => ((d, p) -> mean(p, weights(1 ./ (d.^3)))) => :pop)
df2.index = string.(df2.h3, base=16)

addquantiles!(df, column; jiggle=false) = begin
    if (!jiggle) 
        raw = ecdf(df[!, column]).(df[!, column])
        raw = raw .- minimum(raw)
        raw = raw ./ maximum(raw)
        return df[!, Symbol(string(column) * "_quantile")] = raw
    end
    l = size(df,1)
    tdf = copy(df[!, [column]])
    tdf.id = 1:l
    sort!(tdf, column)
    tdf.q = (1:l)./l
    sort!(tdf, :id)
    return df[!, Symbol(string(column) * "_quantile")] = tdf.q
end

addquantiles!(df2, :pop)
df2.value = df2.pop_quantile

using CSV
CSV.write("../H3-MON/www/data/h3_data.csv", df2[!, [:index, :value, :pop]])
# http://localhost:1983/#x=178.47055156540932&y=-18.11339853004901&z=12.399161459599881 - fiji


# trying to fix up the lower resolutions
function writehivedir2(outdir, df, groupkeys=[]; filename="part0.arrow") # arquero doesn't support zstd
    g = groupby(df, groupkeys)
    for t in keys(g)
        !all(v -> >:(AbstractString, typeof(v)), values(t)) && throw("All grouped column values must be strings") # TODO: support other types?
        path = join(["$k=$v" for (k,v) in zip(keys(t), values(t))], "/")
        mkpath(joinpath(outdir,path))
        Arrow.write(joinpath(outdir,path,filename), g[t][!, Not(keys(t))])
    end
end
df4 = readhivedir("../H3-MON/www/data/JRC_POPULATION_2018_H3_by_rnd")
df4.h3 = parse.(UInt64, df4.index, base=16)
df4.population = df4.real_value .* hexAreaKm2.(parse.(Int,df4.res))
df9 = df4[df4.res .== "9", :]
df9.h3_7 = h3ToParent.(df9.h3, 7)
df9.h3_7 = h3ToParent.(df9.h3, 5)

# df_test = combine(groupby(df9, :h3_7), :population => sum => :population)
# df7 = df4[df4.res .== "7", :]
# leftjoin!(df7, df_test, on=:h3=>:h3_7, makeunique=true) # so it's a pretty big overestimate

# df7.pop_density = df7.population_1 ./ hexAreaKm2.(7)
df7 = combine(groupby(df9, :h3_7), :population => sum => :population)
df5 = combine(groupby(df9, :h3_7), :population => sum => :population)
df5.real_value = df5.population ./ hexAreaKm2.(5)
df7.real_value = df7.population ./ hexAreaKm2.(7)


# this doesn't work :(
#
# i think we probably need to go back to the source file and use the "true" h3 tiles without the kRing interpolation
table = Shapefile.Table("./data/JRC_POPULATION_2018.shp")

df = DataFrame(table)

# geostat is CRS3035

eu2latlon = Proj.Transformation("EPSG:3035", "EPSG:4326")

# eu2latlon(3215009.376512329, 3597518.130827671) # output is definitely lat/lon

point2latlon(p) = eu2latlon(p.y, p.x)
points2mid(ps) = (getfield.(ps, 1), getfield.(ps, 2)) .|> mean

df.midpoint = ThreadsX.map(ps -> points2mid(point2latlon.(ps)), getfield.(df[!, :geometry], :points)) # threadsx was overkill for this it was stupid quick

mini_df = copy(df[!, [:midpoint, :TOT_P_2018, :OBJECTID, :CNTR_ID]]) 
# run this for 7 and then 5
mini_df.h3_7  = map(m -> geoToH3(GeoCoord(deg2rad.(m)...), 7), mini_df.midpoint)
consolidated = combine(groupby(mini_df, :h3_7), :TOT_P_2018 => sum => :population, :TOT_P_2018 => mean => :TOT_P_2018, :CNTR_ID => first => :CNTR_ID)
sort!(consolidated, :TOT_P_2018)
consolidated.pop_quantile = cumsum(consolidated.TOT_P_2018) ./ sum(consolidated.TOT_P_2018)
consolidated.value = consolidated.pop_quantile
consolidated.real_value = consolidated.TOT_P_2018
consolidated.h3_3 = string.(h3ToParent.(consolidated.h3_7, 3), base=16)
consolidated.index = string.(consolidated.h3_7, base=16)
# change 7 to 5 when needed
writehivedir2("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/res=7/", consolidated[!, [:index, :value, :real_value, :population, :h3_3]], [:h3_3])


# updating population for res=9
df9 = readhivedir("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/res=9/")
df9.population = df9.real_value .* hexAreaKm2.(parse.(Int,df9.res))
# DON'T WRITE TO THE SAME DIRECTORY
writehivedir2("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/res=9_2/", df9[!, [:index, :value, :real_value, :population, :h3_3]], [:h3_3])


# how did these end up ints
df9 = readhivedir("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/res=5/")
df9.population = Float64.(df9.population)
# DON'T WRITE TO THE SAME DIRECTORY
writehivedir2("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/res=5_2/", df9[!, [:index, :value, :real_value, :population, :h3_3]], [:h3_3])

# swap h3_3 for h3_1
df9 = readhivedir("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/res=5/")
df9.h3_parent = string.(h3ToParent.(parse.(UInt64, df9.index, base=16), 1), base=16)
combine(groupby(df9, :h3_1), nrow)
writehivedir2("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/res=5_2/", df9[!, [:index, :value, :real_value, :population, :h3_parent]], [:h3_parent])

df = readhivedir("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/")
df.h3_parent = df.h3_3
writehivedir2("../population-around-a-tile/map/data/jrc2/", df[!, [:index, :value, :real_value, :population, :h3_parent, :res]], [:res, :h3_parent])


# progress with GHS 1km^2
# written to ghs_1km.arrow, 1km^2 centroids + population in that square
# just need to rerun the shapefile stuff
using Arrow
df = Arrow.Table("ghs_1km.arrow") |> DataFrame
df.id = 1:size(df, 1)
mini_df = df
 
rings = mapreduce(k -> map(midpoint -> (id=midpoint[3], h3=kRing(geoToH3(GeoCoord(deg2rad.(midpoint[1:2])...), 9), k), dist=k+1), zip(mini_df.lat,mini_df.lon, df.id)), vcat, 0:2) |> DataFrame
rings = flatten(rings, :h3)

# sort!(rings, :dist) # comes presorted
rings = rings[.!nonunique(rings, [:h3, :id], keep=:first), :]
leftjoin!(rings, mini_df[!, [:pop, :id]], on=:id)

# weighted average with centre = 1, nthring = 1/dist^3
df2 = combine(groupby(rings, :h3), [:dist, :pop] => ((d, p) -> mean(p, weights(1 ./ (d.^3)))) => :pop)
df2.res .= 9
df2.h3_3 = ThreadsX.map(h -> h3ToParent(h, 3), df2.h3)
rename!(df2, :h3_3 => :h3_3i)
rename!(df2, :pop => :value)
Arrow.write("ghs_h3_9.arrow", df2)
df2.h3_3 .= ThreadsX.map(h -> string(h, base=16), df2.h3_3i)
transform!(groupby(df2, :h3_3i), :h3_3i => x -> string.(x, base=16) => :h3)
# GC.gc()
# df2.index = string.(df2.h3, base=16)
using Arrow, DataFrames
import H3.API: kRing, geoToH3, GeoCoord, h3ToParent, hexAreaKm2
df = Arrow.Table("ghs_h3_9.arrow") |> DataFrame
transform!(groupby(df, :h3_3i), :h3_3i => (x -> string(x[1], base=16)) => :h3_3)
using ArrowHivePartitioner
function writehivedir2(outdir, df, groupkeys=[]; filename="part0.arrow") # arquero doesn't support zstd
    g = groupby(df, groupkeys)
    for t in keys(g)
        !all(v -> >:(AbstractString, typeof(v)), values(t)) && throw("All grouped column values must be strings") # TODO: support other types?
        path = join(["$k=$v" for (k,v) in zip(keys(t), values(t))], "/")
        mkpath(joinpath(outdir,path))
        Arrow.write(joinpath(outdir,path,filename), g[t][!, Not(keys(t))])
    end
end
writehivedir2("ghs_hive/", df[!, [:h3, :value, :res, :h3_3]], [:h3_3])

# todo: rename h3_3 to h3_parent, res=9 to folder
# convert h3 -> index string (will need to do one at a time really)
# .. tbh probably actually better to do this for res=7, res=5 first? could even say "high res data only available for europe". total size would be 10GB otherwise, too big for github



# can't remember any of this stuff
using ArrowHivePartitioner
using Arrow, DataFrames
import H3, Plots, ThreadsX
# import H3.API: kRing, geoToH3, GeoCoord, h3ToParent, hexAreaKm2, h3Distance, edgeLengthKm # eugh a version update changed all these
import H3.API: latLngToCell, LatLng, hexAreaKm2, gridDistance, edgeLengthKm, gridDisk
df = readhivedir("../population-around-a-tile/map/data/JRC_POPULATION_2018_H3_by_rnd/res=7/")
df.h3 = parse.(UInt64, df.index, base=16)

# [sum(df[in.(df.h3, Ref(kRing(df.h3[1],i))), :population]) for i in 1:10] # this is annoyingly slow, why is it so slow -> maybe better to do it with a distance thing first

function popatdist(lonlatstr, distances=0:60)
    # lonlatstr = "8.540368 47.378156"
    lonlat = deg2rad.(parse.(Float64, split(lonlatstr)))
    # distances = 0:10
    # candidates = gridDisk(latLngToCell(LatLng(lonlat[2], lonlat[1]), 9), distances[end]) # in check against this seems to be too slow
    df.dist = ThreadsX.map(h -> gridDistance(h, latLngToCell(LatLng(lonlat[2], lonlat[1]), 7)), df.h3)
    # radius_mult = edgeLengthKm(9) # broken?
    # radius_mult = 0.174375668 
    radius_mult = 3 # guesstimate
    # gridDistance(df.h3[end], df.h3[1]) # regression in H3. this used to be -1
    df[(df.dist .== 0) .&& (df.h3 .!= df.h3[1]), :dist] .= -1
    mini = copy(df[distances[1] .<= df.dist .<= distances[end], :])
    sort!(mini, :dist)
    mini.cumpop = cumsum(mini.population)
    plt = mini[.!nonunique(mini, :dist, keep=:last), :]
    return (radius_mult .* plt.dist, plt.cumpop ./ 1_000_000 )
end
# grab from https://epsg.io/map#srs=4326&x=4.900615&y=52.378937&z=15&layer=streets
zurich = popatdist("8.540368 47.378156")
p = Plots.plot(zurich..., label="Zurich", ylabel="Population (millions)", xlabel="Distance (km)")
paris = popatdist("2.348928 48.853252")
Plots.plot!(p, paris..., label="Paris")
london = popatdist("-0.126368 51.532502")
Plots.plot!(p, london..., label="London")
munich = popatdist("11.557102 48.140284")
Plots.plot!(p, munich..., label="Munich")
brussels = popatdist("4.336029 50.835107")
Plots.plot!(p, brussels..., label="Brussels")
# amsterdam = popatdist("4.900615 52.378937")
# Plots.plot!(p, amsterdam..., label="Amsterdam")
bordeaux = popatdist("-0.590343 44.857816")
Plots.plot!(p, bordeaux..., label="Bordeaux")
# lyon = popatdist("4.860058 45.760454")
# Plots.plot!(p, lyon..., label="Lyon")
strasbourg = popatdist("7.734182 48.585306")
Plots.plot!(p,strasbourg..., label="Strasbourg")
manchester = popatdist("-2.229845 53.477218")
Plots.plot!(p, manchester..., label="Manchester")
