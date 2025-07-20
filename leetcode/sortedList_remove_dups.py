def remove_dupes(l:list):
    dedup = set()
    for i in range(len(l)-1):
        if l[i] not in dedup:
            #print(dedup)
            dedup.add(l[i])
        else:
            while l[i] in dedup:
                dedup.remove(l[i])
            dedup.add(l[i])
    return dedup

print(remove_dupes([0,0,1,1,1,2,2,3,3,4]))


