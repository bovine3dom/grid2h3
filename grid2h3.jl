using Shapefile, DataFrames, Statistics, RollingFunctions, Proj, ThreadsX, StatsBase, CSV
import H3.API: kRing, geoToH3, GeoCoord


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
df3 = df[df.CNTR_ID .== "UK", :]
#mini_df = copy(df[end-(stripe_size*stripe_number):end-((stripe_number-1)*stripe_size), [:midpoint, :TOT_P_2018, :OBJECTID]]) # 300_000 is hard for computer :(
mini_df = copy(df3[!, [:midpoint, :TOT_P_2018, :OBJECTID]]) 

# resolution 9: at distance 2 it covers about 2km^2
rings = mapreduce(k -> map(midpoint -> (OBJECTID=midpoint[2], h3=kRing(geoToH3(GeoCoord(deg2rad.(midpoint[1])...), 9), k), dist=k+1), zip(mini_df.midpoint, mini_df.OBJECTID)), vcat, 0:2) |> DataFrame
rings = flatten(rings, :h3)

# do it the "right way"
#combine(mini_df, :midpoint => x -> map(p -> (h3=kRing(geoToH3(GeoCoord(deg2rad.(p)...), 9), 2),), x))
#mapreduce(k -> combine(mini_df, AsTable(:) => ByRow(row->(OBJECTID=row.OBJECTID,h3=kRing(geoToH3(GeoCoord(deg2rad.(row.midpoint)...), 9), k), dist=k+1,)) => AsTable), vcat, 0:2) # after all that hassle the one above is quicker and does the same thing, story of my life


# sort!(rings, :dist) # comes presorted
rings = rings[.!nonunique(rings, [:h3, :OBJECTID], keep=:first), :]
leftjoin!(rings, mini_df[!, [:TOT_P_2018, :OBJECTID]], on=:OBJECTID)

# weighted average with centre = 1, nthring = 1/dist^3
df2 = combine(groupby(rings, :h3), [:dist, :TOT_P_2018] => ((d, p) -> mean(p, weights(1 ./ (d.^3)))) => :TOT_P_2018)



# gridding ends, this is just for fun / plotting


df2.index = string.(df2.h3, base=16)
# addquantiles!(df2, :TOT_P_2018)
# df2.value = df2.TOT_P_2018_quantile
# CSV.write("$(homedir())/projects/H3-MON/www/data/h3_data.csv", df2[!, [:index, :value]])

# but those are quantiles based on land, to be based on people we need cumsum
sort!(df2, :TOT_P_2018)
df2.pop_quantile = cumsum(df2.TOT_P_2018) ./ sum(df2.TOT_P_2018)
df2.value = df2.pop_quantile
df2.real_value = df2.TOT_P_2018
CSV.write("$(homedir())/projects/H3-MON/www/data/h3_data.csv", df2[!, [:index, :value, :real_value]])

# do i need this?
function densityatquantile(q,poptuple)
    populations, cum_population = poptuple
    quantileth_man(q) = last(cum_population)*q

    quantileth_density(q) = populations[findfirst(x->x>quantileth_man(q), cum_population)]
    quantileth_density(q)
end
