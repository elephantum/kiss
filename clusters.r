library('igraph')

pairs <- read.csv('session_pairs.hive', sep='\x01', header=FALSE, col.names=c('a', 'b'))

g <- graph.edgelist(as.matrix(pairs), directed=FALSE)

gg <- clusters(g)

alias.by.group <- data.frame(group=gg$membership, alias=V(g)$name, stringsAsFactors=FALSE)

alias.by.group$priority <- order(order(
  alias.by.group$group,
  -grepl('^89', alias.by.group$alias),
  -grepl('@', alias.by.group$alias), 
  grepl(' ', alias.by.group$alias)
))

priority.by.group <- aggregate(x=alias.by.group[c('priority')], by=list(group=alias.by.group$group), FUN=min)

main.alias.by.group <- merge(x=priority.by.group, y=alias.by.group, by=c('group', 'priority'))

session.aliases <- merge(
  suffixes=c('.main', ''),
  y=alias.by.group[c('group', 'alias')], 
  x=main.alias.by.group[c('group', 'alias')], 
  by=c('group')
)

write.table(
  file='aliases.csv',
  x=session.aliases[c('alias.main', 'alias')],
  sep=',',
  col.names=FALSE,
  row.names=FALSE
)



# order_by_class <- read.table(file='report_order_total_by_user_class_100k//000000_0.gz', sep='\x01')
# order_by_class_flat <- cast(order_by_class, V1 ~ V2, variable=V4)
# library('reshape')
# order_by_class_flat <- cast(order_by_class, V1 ~ V2, variable=V4)
# barplot(t(as.matrix(order_count_by_class_flat)), legend=rownames(t(as.matrix(order_count_by_class_flat))))
