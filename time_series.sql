select * from DB.coin_list
where platforms = 'solana'
and ID not like '%1x%'
and ID not like '%3x%'
and Id not like '%5x%'