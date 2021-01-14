select "Race","Sex",count(*) as TOTAL from animal
group by 1,2 order by TOTAL desc ;