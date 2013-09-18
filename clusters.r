library('igraph')

pairs <- read.csv('session_pairs.hive', sep='\x01', header=FALSE, col.names=c('a', 'b'))
pairs.matrix <- as.matrix(pairs)

g <- graph.edgelist(pairs.matrix, directed=FALSE)

gg <- clusters(g)

alias.by.group <- data.frame(group=gg$membership, alias=V(g)$name, stringsAsFactors=FALSE)

alias.by.group$priority <- order(order(
  alias.by.group$group, 
  -grepl('@', alias.by.group$alias), 
  grepl(' ', alias.by.group$alias)))

priority.by.group <- aggregate(x=alias.by.group[c('priority')], by=list(group=alias.by.group$group), FUN=min)

main.alias.by.group <- merge(x=priority.by.group, y=alias.by.group, by=c('group', 'priority'))

groups <- merge(x=alias.by.group, y=main.alias.by.group, by=c('group'))