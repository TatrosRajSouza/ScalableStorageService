package common;

public enum Ident {
	CLIENT(1),
	SERVER(2),
	ECS(3);
	
    private final int value;
    
    private Ident(final int value) {
        this.value = value;
    }

    public int getValue() { return this.value; }
}
