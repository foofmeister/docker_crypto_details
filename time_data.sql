SELECT distinct a.ID from DB.coin_list a
 left outer join DB.eligible_coins b on upper(a.SYMBOL) = upper(b.COIN)
 where (b.COIN is not null or PLATFORMS in ('solana'))


