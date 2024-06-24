type Scalar = string | number | boolean | null

type GenericSerializable = Scalar | undefined | void | GenericSerializable[] | { [key: string]: GenericSerializable }

export type Data = GenericSerializable

export type Validator<Out = Data> = {
	parse: (input: any) => Out
}

type Increment<A extends number[]> = [...A, 0]

export type DeepPartial<T, CurrentDepth extends number[] = []> = T extends object ? CurrentDepth["length"] extends 8 ? T : {
	[P in keyof T]?: DeepPartial<T[P], Increment<CurrentDepth>>
} : T