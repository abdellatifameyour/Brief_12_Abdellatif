--Select count (distinct  dimPatientPK) from FactTable
Select * from FactTable
select 
dimPayerPK,
count (*) as nb_trns
from FactTable
group by dimPayerPK


Select 
p.dimPayerPK,
count (*) as nb_trns,
d.PayerName
from FactTable as p
inner join dimPayer as d on p.dimPayerPK=d.dimPayerPK
group by p.dimPayerPK,d.PayerName
having PayerName like 'M%'
ORDER BY nb_trns DESC
