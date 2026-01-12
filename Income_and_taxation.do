describe racecen1
tabulate racecen1
summarize racecen1

summarize wrkstat
tabulate wrkstat

summarize class
tabulate class

Summ education
Tabulate education

summarize satfin
tabulate satfin

tabulate region
summarize region

tabulate hompop
summarize hompop

describe size
histogram size
summarize size

histogram conrinc
summ conrinc

hist age
summ age

tabulate born
summ born

tabulate parborn
summ parborn

describe partyid
gen party_dummy = .
replace party_dummy = 1 if inlist(partyid, 1, 2)
replace party_dummy = 0 if inlist(partyid, 6, 7)
drop if inlist(partyid, 3, 4, 5, 8)
tabulate party_dummy
summarize party_dummy

tabstat conrinc, by(party_dummy) stats(mean median sd min max)
histogram conrinc if party_dummy == 0, color(red) title ("Republicans")
histogram conrinc if party_dummy == 1, color(blue) title ("Democrats")

reg conrinc party_dummy

summ income16, detail
gen lowincome = 1 if income16 <=15
replace lowincome = 0 if lowincome ==.
keep if income16 <= 15 | income16 >=22
ttest tax, by (lowincome)


