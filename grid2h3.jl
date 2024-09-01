using Shapefile, DataFrames, Statistics, RollingFunctions, Proj, ThreadsX, StatsBase, CSV
import H3.API: kRing, geoToH3, GeoCoord, h3ToParent


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
