package core

type Node struct {
	Id    string
	Port  string
	Peers map[string]string
}

func (n *Node) Get(string key) string {

}

func (n *Node) Put(string key, string value) {

}
