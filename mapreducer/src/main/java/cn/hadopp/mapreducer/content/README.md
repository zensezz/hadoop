### hadoop编程之基于内容的推荐算法

​	基于内容的协同过滤推荐算法：给用户推荐和他们之前喜欢的物品在内容上相似的其他物品

#### **物品特征建模（item profile）**

以电影为例

​	1表示电影具有某特征，0表示电影不具有某特征

|            | 科幻 | 言情 | 喜剧 | 动作 | 纪实 | 国产 | 欧美 | 日韩 | 斯嘉丽的约翰 | 成龙 | 范冰冰 |
| ---------- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ------------ | ---- | ------ |
| 复仇者联盟 | 1    | 0    | 0    | 1    | 0    | 0    | 1    | 0    | 1            | 0    | 0      |
| 绿巨人     | 1    | 0    | 0    | 1    | 0    | 0    | 1    | 0    | 0            | 0    | 0      |
| 宝贝计划   | 0    | 0    | 1    | 1    | 0    | 1    | 0    | 0    | 0            | 1    | 1      |
| 十二生肖   | 0    | 0    | 0    | 1    | 0    | 1    | 0    | 0    | 0            | 1    | 0      |

####   **算法步骤** 

- #### 构建item profile矩阵

物品ID——标签

| tag: | (1)  | (2)  | (3)  | (4)  | (5)  | (6)  | (7)  | (8)  | (9)  |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| I1： | 1    | 0    | 0    | 1    | 1    | 0    | 1    | 0    | 0    |
| I2： | 0    | 1    | 0    | 1    | 0    | 0    | 1    | 0    | 1    |
| I3： | 0    | 1    | 1    | 0    | 0    | 1    | 0    | 1    | 1    |
| I4： | 1    | 0    | 1    | 1    | 1    | 0    | 0    | 0    | 0    |
| I5： | 0    | 1    | 0    | 1    | 0    | 0    | 1    | 1    | 0    |

- 构建item user评分矩阵

  用户ID——物品id

|      | I1   | I2   | I3   | I4   | I5   |
| ---- | ---- | ---- | ---- | ---- | ---- |
| U1   | 1    | 0    | 0    | 0    | 5    |
| U2   | 0    | 4    | 0    | 1    | 0    |
| U3   | 0    | 5    | 3    | 0    | 1    |

- item user    X    item profile    = user profile

  用户ID——标签

| tag: | (1)  | (2)  | (3)  | (4)  | (5)  | (6)  | (7)  | (8)  | (9)  |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| U1   | 1    | 5    | 0    | 6    | 1    | 0    | 6    | 5    | 0    |
| U2   | 1    | 4    | 1    | 5    | 1    | 0    | 4    | 0    | 4    |
| U3   | 0    | 9    | 3    | 6    | 0    | 3    | 6    | 4    | 8    |

​	值的含义：用户对所有标签感兴趣的程度

​	比如： U1-（1）表示用户U1对特征（1）的偏好权重为1，可以看出用户U1对特征（4）（7）最感兴趣，其权重为6

- 对user profile 和 item profile求余弦相似度

  左侧矩阵的每一行与右侧矩阵的每一行计算余弦相似度

  cos<U1,I1>表示用户U1对物品I1的喜好程度，最后需要将已有评分的物品置零，不推荐该物品

MapReduce步骤

- 将item profile转置

输入：物品ID（行）——标签ID（列）——0或1        物品特征建模

输出：标签ID（行）——物品ID（列）——0或1



-  item user (评分矩阵)    X    item profile（已转置）

输入：根据用户的行为列表计算的评分矩阵

缓存：步骤1输出

输出：用户ID（行）——标签ID（列）——分值（用户对所有标签感兴趣的程度）

- cos<步骤1输入,步骤2输出>

输入：步骤1输入        物品特征建模

缓存：步骤2输出

输出：用户ID（行）——物品ID（列）——相似度
